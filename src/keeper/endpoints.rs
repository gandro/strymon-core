//! Responsible for handling clients.
use std::collections::{VecDeque, HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::io;
use std::net::{ToSocketAddrs, SocketAddr};
use std::sync::{Arc, Mutex};
use std::any::Any;

use abomonation::Abomonation;
use futures::{Poll, Async};
use futures::stream::{Stream, Fuse};
use futures::executor::{spawn, Spawn, Notify};
use timely::dataflow::{ScopeParent, Stream as TimelyStream};
//use timely::dataflow::Stream as TimelyStream;
use timely::dataflow::operators::{Inspect, Map};
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::operator::source;
use timely::dataflow::scopes::child::Child;
use timely::progress::timestamp::Timestamp;

use timely_system::query::Coordinator;
use timely_system::network::{Network, Listener};
use timely_system::query::keepers::KeeperWorkerRegistrationError;
use timely_system::network::message::abomonate::NonStatic;

use model::{KeeperQuery, KeeperResponse};
use keeper::messenger::Messenger;
use keeper::model::{ClientQuery, QueryResponse};

/// Helper structure that handles accepting clients' requests and responding to them.
pub struct Connector<'a, Q, R, S: ScopeParent, T: Timestamp>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    connections: Arc<Mutex<ConnectionStorage<Q, R>>>,
    acceptor: Arc<Mutex<Spawn<Acceptor<Q, R>>>>,
    in_stream: TimelyStream<Child<'a, S, T>, ClientQuery<Q>>,
    worker_index: usize,
    workers_num: usize,
    // All clients that subscribed to receive updates to the state.
    subscribed_clients: Arc<Mutex<HashMap<u64, HashSet<usize>>>>,
    coord_ref: Option<(String, Coordinator)>,
}

impl<'a, Q, R, S: ScopeParent, T: Timestamp> Connector<'a, Q, R, S, T>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    pub fn new<P: Into<Option<u16>>>(port: P, scope: &mut Child<'a, S, T>) -> io::Result<Self> {
        let worker_index = scope.index();
        let connections = Arc::new(Mutex::new(ConnectionStorage::new()));
        let acceptor = Arc::new(Mutex::new(spawn(Acceptor::new(port)?)));
        let stream = source(scope, "IncomingClients", |capability| {
            let mut capability = Some(capability);
            let acceptor = acceptor.clone();
            let connections = connections.clone();

            move |output| {
                let mut acceptor = acceptor.lock().unwrap();
                let mut connections = connections.lock().unwrap();

                // Using Noop here since we don't need notification.
                let noop = Arc::new(NoopNotify {});
                match acceptor.poll_stream_notify(&noop, 0) {
                    Ok(Async::Ready(Some((query, messenger)))) => {
                        if let Some(cap) = capability.as_mut() {
                            let conn_id = connections.insert_connection(messenger);
                            let element = ClientQuery::new(query, conn_id, worker_index);
                            output.session(&cap).give(element);
                        }
                    }
                    Ok(Async::NotReady) => (),
                    Ok(Async::Ready(None)) => {
                        capability = None;
                    }
                    Err(_) => {
                        // TODO:
                        //  - if the error is fatal (socket broken somehow?) end this stream by
                        //  releasing the capability:
                        //  capability = None;
                        //  - if the error is not fatal (just connection to one client broke) count
                        //  it somewhere but ignore otherwise
                        ()
                    }
                };
            }
        });
        Ok(Connector {
               connections: connections,
               acceptor: acceptor,
               in_stream: stream,
               worker_index: scope.index(),
               workers_num: scope.peers(),
               subscribed_clients: Arc::new(Mutex::new(HashMap::new())),
               coord_ref: None,
           })
    }

    pub fn external_addr(&self) -> SocketAddr {
        self.acceptor
            .lock()
            .unwrap()
            .get_ref()
            .external_addr()
    }

    pub fn incoming_stream(&self) -> TimelyStream<Child<'a, S, T>, ClientQuery<Q>> {
        self.in_stream.clone()
    }

    pub fn outgoing_stream(&mut self,
                           out_stream: TimelyStream<Child<'a, S, T>, QueryResponse<R>>) {
        let connections = self.connections.clone();
        let subscribed_clients = self.subscribed_clients.clone();
        let workers_num = self.workers_num.clone();
        out_stream.flat_map(move |cqr: QueryResponse<R>| {
                // If we get broadcast response, copy it over to all workers.
                let mut resp = Vec::new();
                if let &QueryResponse::Broadcast(ref msg) = &cqr {
                    for x in 0..workers_num {
                        let mut cloned = msg.clone();
                        cloned.set_target_worker_idx(x);
                        resp.push(QueryResponse::Broadcast(cloned));
                    }
                }
                if !cqr.is_broadcast() {
                    resp.push(cqr);
                }
                resp
            })
            .exchange(|cqr: &QueryResponse<R>| cqr.route_to())
            .inspect(move |cqr| {
                let mut subscribed_clients = subscribed_clients.lock().unwrap();
                let mut connections = connections.lock().unwrap();

                match cqr {
                    &QueryResponse::Broadcast(ref msg) => {
                        subscribed_clients.retain(|&idx, ref mut workers| {
                            if !workers.contains(&msg.source_worker_idx()) {
                                return true;
                            }
                            let mut to_send = msg.response_tuples().clone();
                            to_send.push(KeeperResponse::BatchEnd);
                            match connections.entry(idx) {
                                Entry::Occupied(connection) => {
                                    if let Err(err) = connection.get()
                                           .send_message(to_send.clone()) {
                                        warn!("Disconnected from a client with error: '{}'", err);
                                        // Something went wrong while communicating with the
                                        // client, we assume they disconnected.
                                        connection.remove_entry();
                                        return false;
                                    }
                                    true
                                }
                                Entry::Vacant(_) => false,
                            }
                        });
                    }
                    &QueryResponse::Unicast(ref msg) => {
                        let conn_id = msg.client_details().connection_id();
                        match connections.entry(conn_id) {
                            Entry::Occupied(connection) => {
                                let mut errored = false;
                                let mut to_send = msg.response_tuples().clone();
                                if msg.subscribe() {
                                    to_send.push(KeeperResponse::BatchEnd);
                                } else {
                                    to_send.push(KeeperResponse::ConnectionEnd);
                                }
                                if let Err(err) = connection.get().send_message(to_send) {
                                    warn!("Error on connection to client: {:?}", err);
                                    errored = true;
                                }
                                if !msg.subscribe() || errored {
                                    // If it was a point query/single request close the connection.
                                    connection.remove_entry();
                                }
                            }
                            Entry::Vacant(_) => {
                                warn!("Trying to send something to non-existing client");
                            }
                        }
                        if msg.subscribe() {
                            subscribed_clients.entry(conn_id).or_insert_with(|| {
                                let mut set = HashSet::new();
                                set.insert(msg.source_worker_idx());
                                set
                            });
                        }
                    }
                };
            });
    }

    pub fn register_with_coordinator(&mut self,
                                     name: &str,
                                     coord: &Coordinator)
                                     -> Result<(), KeeperWorkerRegistrationError> {
        coord.add_keeper_worker(name, self.worker_index, self.external_addr())?;
        self.coord_ref = Some((name.to_string(), coord.clone()));
        Ok(())
    }
}

// Bad idea - connector is dropped before dataflow starts.
//impl<'a, Q, R, S: ScopeParent, T: Timestamp> Drop for Connector<'a, Q, R, S, T>
//    where Q: Abomonation + Any + Clone + NonStatic,
//          R: Abomonation + Any + Clone + Send + NonStatic {
//
//    fn drop(&mut self) {
//        if let Some((ref name, ref coord)) = self.coord_ref {
//            if let Err(err) = coord.remove_keeper_worker(name, self.worker_index) {
//                warn!("Unsuccessful deregistration: {:?}", err);
//            }
//        }
//    }
//}


/// Helper struct for storing user connections.
struct ConnectionStorage<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    // connections is a HashMap of user connections indexed by u64. A new user connection simply
    // gets id by adding one to previous id being used (wrapping to 0 if we hit max u64). Since we
    // use u64 we should never fall into already taken place, but should check for that in future.
    connections: HashMap<u64, Messenger<KeeperQuery<Q>, Vec<KeeperResponse<R>>>>,
    next_conn_id: u64,
}

impl<Q, R> ConnectionStorage<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    fn new() -> Self {
        ConnectionStorage {
            connections: HashMap::new(),
            next_conn_id: 0,
        }
    }

    fn insert_connection(&mut self,
                         conn: Messenger<KeeperQuery<Q>, Vec<KeeperResponse<R>>>)
                         -> u64 {
        self.connections.insert(self.next_conn_id, conn);
        let idx = self.next_conn_id;
        self.next_conn_id = self.next_conn_id.wrapping_add(1);
        idx
    }

    fn entry(&mut self, key: u64) -> Entry<u64, Messenger<KeeperQuery<Q>, Vec<KeeperResponse<R>>>> {
        self.connections.entry(key)
    }
}

/// Accept and unwrap clients' requests.
struct Acceptor<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    listener: Fuse<Listener>,
    addr: SocketAddr,
    pending_clients: VecDeque<Messenger<KeeperQuery<Q>, Vec<KeeperResponse<R>>>>,
    pending_queries: VecDeque<(KeeperQuery<Q>, Messenger<KeeperQuery<Q>, Vec<KeeperResponse<R>>>)>,
}

impl<Q, R> Acceptor<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    fn new<P: Into<Option<u16>>>(port: P) -> io::Result<Self> {
        let network = Network::init()?;
        let listener = network.listen(port)?;
        let addr = match listener.external_addr()
                  .to_socket_addrs()?
                  .next() {
            Some(addr) => addr,
            None => {
                return Err(io::Error::new(io::ErrorKind::Other, "Server returned wrong address"));
            }
        };
        Ok(Acceptor {
               listener: listener.fuse(),
               addr: addr,
               pending_clients: VecDeque::new(),
               pending_queries: VecDeque::new(),
           })
    }

    fn external_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Handshake all incoming client connections.
    fn poll_clients(&mut self) -> io::Result<()> {
        // TODO important! what if listener poll returns error? what does it mean?
        while let Async::Ready(Some((tx, rx))) = self.listener.poll()? {
            self.pending_clients.push_back(Messenger::new(tx, rx));
        }
        Ok(())
    }

    /// Get details of their query from all waiting clients.
    fn poll_queries(&mut self) -> io::Result<()> {
        for _ in 0..self.pending_clients.len() {
            if let Some(mut messenger) = self.pending_clients.pop_front() {
                // We expect to receive only one message - the query.
                match messenger.poll() {
                    Ok(Async::Ready(Some(query))) => {
                        self.pending_queries.push_back((query, messenger));
                    }
                    Ok(Async::NotReady) => self.pending_clients.push_back(messenger),
                    // None or Err means that client disconnected.
                    Ok(Async::Ready(None)) => (),
                    Err(_) => (),
                }
            } else {
                unreachable!();
            }
        }
        Ok(())
    }
}

impl<Q, R> Stream for Acceptor<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    type Item = (KeeperQuery<Q>, Messenger<KeeperQuery<Q>, Vec<KeeperResponse<R>>>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if !self.listener.is_done() {
            self.poll_clients()?;
        }
        if !self.pending_clients.is_empty() {
            self.poll_queries()?;
        }
        match self.pending_queries.pop_front() {
            Some(el) => Ok(Async::Ready(Some(el))),
            None => {
                if self.listener.is_done() {
                    Ok(Async::Ready(None))
                } else {
                    Ok(Async::NotReady)
                }
            }
        }
    }
}

struct NoopNotify {}

impl Notify for NoopNotify {
    fn notify(&self, _: usize) {}
}


#[cfg(test)]
mod tests {
    use std::thread;
    use std::net::ToSocketAddrs;
    use futures::Stream;

    use timely;
    use timely::dataflow::operators::Map;
    use timely::dataflow::operators::Inspect;

    use timely_system::network::Network;
    use timely_system::network::message::MessageBuf;
    use timely_system::network::message::abomonate::Abomonate;

    use keeper::model::QueryResponse;
    use model::{KeeperQuery, KeeperResponse};
    use super::{Acceptor, Connector};

    #[test]
    fn test_acceptor() {
        let acceptor = Acceptor::<String, String>::new(None).unwrap();
        let addr = acceptor.external_addr();

        let network = Network::init().unwrap();
        let (client_tx, client_rx) = network.connect(addr).unwrap();

        let client_thread = thread::spawn(move || {
            let qstr = "Testing".to_string();
            // Send query.
            let mut buf = MessageBuf::empty();
            buf.push::<Abomonate, KeeperQuery<String>>(&KeeperQuery::Query(qstr.clone())).unwrap();
            client_tx.send(buf);

            // Receive reqponse.
            let resp_buf = client_rx.wait().next().unwrap();
            let mut resp_buf = resp_buf.unwrap();
            let resp = resp_buf.pop::<Abomonate, Vec<KeeperResponse<String>>>().unwrap();
            assert_eq!(resp, vec![KeeperResponse::Response(qstr)]);
        });

        for conn in acceptor.take(1).wait() {
            let str_msg = "Testing".to_string();
            let (query, messenger) = conn.unwrap();
            assert_eq!(query, KeeperQuery::Query(str_msg.clone()));
            messenger.send_message(vec![KeeperResponse::Response(str_msg)]).unwrap();
        }
        let _ = client_thread.join();
    }

    /// This test assumes that port `port` is open.
    /// It is ignored since it will run indefinitely.
    /// TODO: remove or fix
    #[ignore]
    #[test]
    fn test_connector() {
        timely::execute(timely::Configuration::Thread, |root| {
            let port = 53545;
            let addr = ("localhost", port)
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap();

            root.dataflow::<(), _, _>(|scope| {
                let mut connector = Connector::<String, String, _, _>::new(Some(port), scope)
                    .unwrap();
                let stream = connector.incoming_stream();
                stream.inspect(|x| println!("got: {:?}", x.query()));
                let stream =
                    stream.map(|cq| {
                                   let mut cr = QueryResponse::unicast(&cq, false, 0);
                                   cr.add_tuple(KeeperResponse::Response("Testing".to_string()));
                                   cr
                               });
                connector.outgoing_stream(stream);
            });

            let network = Network::init().unwrap();
            let (client_tx, client_rx) = network.connect(addr).unwrap();
            let client_thread = thread::spawn(move || {
                let query = KeeperQuery::Query("Testing".to_string());
                // Send query.
                let mut buf = MessageBuf::empty();
                buf.push::<Abomonate, KeeperQuery<String>>(&query).unwrap();
                client_tx.send(buf);

                // Receive reqponse.
                let resp_buf = client_rx.wait().next().unwrap();
                let mut resp_buf = resp_buf.unwrap();
                let resp = resp_buf.pop::<Abomonate, KeeperResponse<String>>().unwrap();
                assert_eq!(resp, KeeperResponse::Response("Testing".to_string()));
            });

            while root.step() {}

            let _ = client_thread.join();
        })
                .unwrap();
    }
}
