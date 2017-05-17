//! Responsible for handling clients.
use std::collections::{VecDeque, HashMap};
use std::io;
use std::net::{ToSocketAddrs, SocketAddr};
use std::sync::{Arc, Mutex};

use futures::{Poll, Async};
use futures::stream::{Stream, Fuse};
use futures::executor::{spawn, Spawn, Unpark};
use timely::dataflow::{ScopeParent, Stream as TimelyStream};
//use timely::dataflow::Stream as TimelyStream;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::operator::source;
use timely::dataflow::scopes::child::Child;
use timely::progress::timestamp::Timestamp;

use timely_system::network::{Network, Listener};

use core::messenger::Messenger;
use core::data::{ClientQuery, ClientQueryResponse};
use super::{Query, ResponseTuple};

/// Helper structure that handles accepting clients' requests and responding to them.
pub struct Connector<'a, S: ScopeParent, T: Timestamp> {
    connections: Arc<Mutex<ConnectionStorage>>,
    acceptor: Arc<Mutex<Spawn<Acceptor>>>,
    in_stream: TimelyStream<Child<'a, S, T>, ClientQuery>,
}

// TODO:
//  - make Coordinator aware of multiple workers (and thus multiple copies of the same Keeper)
//  - add statistics of what has been removed
//  - keep something more complicated in connections
//
//  - work only on worker 0
//  - split this file into multiple smaller


impl<'a, S: ScopeParent, T: Timestamp> Connector<'a, S, T> {
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
                let noop = Arc::new(NoopUnpark {});
                match acceptor.poll_stream(noop) {
                    Ok(Async::Ready(Some((query, messenger)))) => {
                        if let Some(cap) = capability.as_mut() {
                            let conn_id = connections.insert_connection(messenger);
                            let element = ClientQuery::new(&query, conn_id, worker_index);
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
           })
    }

    pub fn external_addr(&self) -> SocketAddr {
        self.acceptor
            .lock()
            .unwrap()
            .get_ref()
            .external_addr()
    }

    pub fn incoming_stream(&self) -> TimelyStream<Child<'a, S, T>, ClientQuery> {
        self.in_stream.clone()
    }

    pub fn outgoing_stream(&mut self,
                           out_stream: TimelyStream<Child<'a, S, T>, ClientQueryResponse>) {
        let connections = self.connections.clone();
        out_stream.exchange(|cqr: &ClientQueryResponse| cqr.worker_index() as u64)
            .inspect(move |cqr| {
                let idx = cqr.connection_id();
                let connections = connections.lock().unwrap();
                // TODO do something when client disconnects
                match connections.get_connection(&idx) {
                    Some(connection) => {
                        let _ = connection.send_message(ResponseTuple::new(&cqr.response()));
                    }
                    None => (),
                }
            });
    }
}

/// Helper struct for storing user connections.
struct ConnectionStorage {
    // connections is a HashMap of user connections indexed by u64. A new user connection simply
    // gets id by adding one to previous id being used (wrapping to 0 if we hit max u64). Since we
    // use u64 we should never fall into already taken place, but should check for that in future.
    connections: HashMap<u64, Messenger<Query, ResponseTuple>>,
    next_conn_id: u64,
}

impl ConnectionStorage {
    fn new() -> Self {
        ConnectionStorage {
            connections: HashMap::new(),
            next_conn_id: 0,
        }
    }

    fn insert_connection(&mut self, conn: Messenger<Query, ResponseTuple>) -> u64 {
        self.connections.insert(self.next_conn_id, conn);
        let idx = self.next_conn_id;
        self.next_conn_id = self.next_conn_id.wrapping_add(1);
        idx
    }

    fn get_connection(&self, key: &u64) -> Option<&Messenger<Query, ResponseTuple>> {
        self.connections.get(key)
    }

    fn remove_connection(&mut self, key: &u64) {
        self.connections.remove(key);
    }
}

/// Accept and unwrap clients' requests.
struct Acceptor {
    listener: Fuse<Listener>,
    addr: SocketAddr,
    pending_clients: VecDeque<Messenger<Query, ResponseTuple>>,
    pending_queries: VecDeque<(String, Messenger<Query, ResponseTuple>)>,
}

impl Acceptor {
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
                    Ok(Async::Ready(Some(Query { text: query }))) => {
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

impl Stream for Acceptor {
    type Item = (String, Messenger<Query, ResponseTuple>);
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

struct NoopUnpark {}

impl Unpark for NoopUnpark {
    fn unpark(&self) {}
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
    use core::{Query, ResponseTuple};
    use super::{Acceptor, Connector};

    #[test]
    fn test_acceptor() {
        let acceptor = Acceptor::new(None).unwrap();
        let addr = acceptor.external_addr();

        let network = Network::init().unwrap();
        let (client_tx, client_rx) = network.connect(addr).unwrap();

        let client_thread = thread::spawn(move || {
            let query = Query::new("Testing");
            // Send query.
            let mut buf = MessageBuf::empty();
            buf.push::<Abomonate, Query>(&query).unwrap();
            client_tx.send(buf);

            // Receive reqponse.
            let resp_buf = client_rx.wait().next().unwrap();
            let mut resp_buf = resp_buf.unwrap();
            let resp = resp_buf.pop::<Abomonate, ResponseTuple>().unwrap();
            assert_eq!(resp.text, "Testing".to_string());
        });

        for conn in acceptor.take(1).wait() {
            let (query, messenger) = conn.unwrap();
            assert_eq!(query, "Testing".to_string());
            messenger.send_message(ResponseTuple::new("Testing")).unwrap();
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
                let mut connector = Connector::new(Some(port), scope, 0).unwrap();
                let stream = connector.incoming_stream();
                stream.inspect(|x| println!("got: {}", x.query()));
                let stream = stream.map(|cq| cq.create_response("Testing"));
                connector.outgoing_stream(stream);
            });

            let network = Network::init().unwrap();
            let (client_tx, client_rx) = network.connect(addr).unwrap();
            let client_thread = thread::spawn(move || {
                let query = Query::new("Testing");
                // Send query.
                let mut buf = MessageBuf::empty();
                buf.push::<Abomonate, Query>(&query).unwrap();
                client_tx.send(buf);

                // Receive reqponse.
                let resp_buf = client_rx.wait().next().unwrap();
                let mut resp_buf = resp_buf.unwrap();
                let resp = resp_buf.pop::<Abomonate, ResponseTuple>().unwrap();
                assert_eq!(resp.text, "Testing".to_string());
            });

            while root.step() {}

            let _ = client_thread.join();
        })
                .unwrap();
    }
}
