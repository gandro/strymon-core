//! Responsible for handling clients.
use std::collections::VecDeque;
use std::io;
use std::net::{ToSocketAddrs, SocketAddr};
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use abomonation::Abomonation;
use futures::{Poll, Async};
use futures::stream::{Stream, Fuse};
use futures::executor::{spawn, Spawn, Unpark};
use timely::dataflow::{Scope, Stream as TimelyStream};
//use timely::dataflow::Stream as TimelyStream;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::operator::source;

use timely_system::network::Network;
use timely_system::network::reqrep::{Server, Incoming, Outgoing, Responder};

use super::{Query, Response};

#[derive(Clone, Debug)]
pub struct ClientQuery {
    query: String,
    /// connection_id is valid only in the context of the same worker, so ClientQuery needs to know
    /// what worker_index it is binded to.
    connection_id: usize,
    /// worker_index is used to make sure this query is directed to outgoing stream on the same
    /// worker as it was created, and thus sent to the correct client.
    worker_index: usize,
}

unsafe_abomonate!(ClientQuery);

impl ClientQuery {
    pub fn new(query: &str, connection_id: usize, worker_index: usize) -> Self {
        ClientQuery {
            query: query.to_string(),
            connection_id: connection_id,
            worker_index: worker_index,
        }
    }

    pub fn query(&self) -> String {
        self.query.to_string()
    }
}

/// This needs to be created with the ClientQuery that this is the response for.
#[derive(Clone, Debug)]
pub struct ClientQueryResponse {
    response: String,
    connection_id: usize,
    worker_index: usize,
}

unsafe_abomonate!(ClientQueryResponse);

impl ClientQueryResponse {
    pub fn new(response: &str, cq: &ClientQuery) -> Self {
        ClientQueryResponse {
            response: response.to_string(),
            connection_id: cq.connection_id,
            worker_index: cq.worker_index,
        }
    }
}

/// Helper structure that handles accepting clients' requests and responding to them.
///
/// It's implementation of Iterator trait hides all the errors from clients and produces
pub struct Connector<S: Scope> {
    // TODO important!! change Vec (that we will run out of in case of a big number of connections)
    // to something better, like HashMap with generated random indexes? (then the questions is how
    // to make sure that we don't have any stragglers and then put a new one in that place)
    connections: Arc<Mutex<Vec<Responder<Query>>>>,
    acceptor: Arc<Mutex<Spawn<Acceptor>>>,
    in_stream: TimelyStream<S, ClientQuery>,
}

// TODO:
//  - make Coordinator aware of multiple workers (and thus multiple copies of the same Keeper)
//  - remove inactive connections (if a client drops off before we responded)
//  - add statistics of what has been removed
//  - keep something more complicated in connections
//  - maybe make it into Stream and Sink (futures)
//
//  - change connections into a map (it still doesn't solve everything)
//  - change sebastian's request responder into something with streaming responses
//  - work only on worker 0
//  - move response creation to ClientQuery (so we do not need to import more stuff)


impl<S: Scope> Connector<S> {
    pub fn new<P: Into<Option<u16>>>(port: P,
                                     scope: &mut S,
                                     worker_index: usize)
                                     -> io::Result<Self> {
        let connections = Arc::new(Mutex::new(Vec::new()));
        let acceptor = Arc::new(Mutex::new(spawn(Acceptor::new(port)?)));
        let stream = source(scope, "IncomingClients", |capability| {
            let mut capability = Some(capability);
            let acceptor = acceptor.clone();
            let connections = connections.clone();

            move |output| {
                let mut acceptor = acceptor.lock().unwrap();
                let mut connections = connections.lock().unwrap();

                // Putting Noop here since we don't need notification.
                let noop = Arc::new(NoopUnpark {});
                match acceptor.poll_stream(noop) {
                    Ok(Async::Ready(Some((query, receiver)))) => {
                        connections.push(receiver);
                        let element = ClientQuery::new(&query, connections.len() - 1, worker_index);
                        let mut session = output.session(capability.as_ref().unwrap());
                        session.give(element);
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

    pub fn incoming_stream(&self) -> TimelyStream<S, ClientQuery> {
        self.in_stream.clone()
    }

    pub fn outgoing_stream(&mut self, out_stream: TimelyStream<S, ClientQueryResponse>) {
        let connections = self.connections.clone();
        out_stream.exchange(|cqr: &ClientQueryResponse| cqr.worker_index as u64)
            .inspect(move |cqr| {
                let idx = cqr.worker_index;
                // TODO Fix this! this assumes that the connection for the given client actually
                // exists!
                let mut conns = connections.lock().unwrap();
                conns.drain(idx..idx + 1)
                    .next()
                    .unwrap()
                    .respond(Ok(Response(cqr.response.to_string())));
            });
    }
}

/// Accept and unwrap clients' requests.
struct Acceptor {
    server: Fuse<Server>,
    addr: SocketAddr,
    pending_clients: VecDeque<(Outgoing, Incoming)>,
    pending_queries: VecDeque<(String, Responder<Query>)>,
}

impl Acceptor {
    fn new<P: Into<Option<u16>>>(port: P) -> io::Result<Self> {
        let network = Network::init()?;
        let server = network.server(port)?;
        let addr = match server.external_addr()
                  .to_socket_addrs()?
                  .next() {
            Some(addr) => addr,
            None => {
                return Err(io::Error::new(io::ErrorKind::Other, "Server returned wrong address"));
            }
        };
        Ok(Acceptor {
               server: server.fuse(),
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
        // TODO important! what if server poll returns error? what does it mean?
        while let Async::Ready(Some(client_tuple)) = self.server.poll()? {
            self.pending_clients.push_back(client_tuple);
        }
        Ok(())
    }

    /// Get details of their query from all waiting clients.
    fn poll_queries(&mut self) -> io::Result<()> {
        for _ in 0..self.pending_clients.len() {
            if let Some((tx, mut rx)) = self.pending_clients.pop_front() {
                // We expect to receive only one message - the query.
                match rx.poll() {
                    Ok(Async::Ready(Some(req))) => {
                        let (Query(query), resp) = req.decode::<Query>()?;
                        self.pending_queries.push_back((query, resp));
                    }
                    Ok(Async::NotReady) => self.pending_clients.push_back((tx, rx)),
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
    type Item = (String, Responder<Query>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if !self.server.is_done() {
            self.poll_clients()?;
        }
        if !self.pending_clients.is_empty() {
            self.poll_queries()?;
        }
        match self.pending_queries.pop_front() {
            Some(el) => Ok(Async::Ready(Some(el))),
            None => {
                if self.server.is_done() {
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
    use futures::{Future, Stream};

    use timely;
    use timely::dataflow::operators::Map;
    use timely::dataflow::operators::Inspect;

    use timely_system::network::Network;
    use core::{Query, Response};
    use super::{Acceptor, Connector, ClientQueryResponse};

    #[test]
    fn test_acceptor() {
        let acceptor = Acceptor::new(None).unwrap();
        let addr = acceptor.external_addr();

        let network = Network::init().unwrap();
        let (client_tx, _) = network.client(addr).unwrap();

        let client_thread = thread::spawn(move || {
            let query = Query("Testing".to_string());
            client_tx.request(&query)
                .and_then(|resp| {
                              assert_eq!(resp.0, "Testing".to_string());
                              Ok(())
                          })
                .wait()
                .unwrap()
        });

        for conn in acceptor.take(1).wait() {
            let (query, resp) = conn.unwrap();
            assert_eq!(query, "Testing".to_string());
            resp.respond(Ok(Response("Testing".to_string())));
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
                stream.inspect(|x| println!("got: {}", x.query));
                let stream = stream.map(|cq| ClientQueryResponse::new("Testing", &cq));
                connector.outgoing_stream(stream);
            });

            let network = Network::init().unwrap();
            let (client_tx, _) = network.client(addr).unwrap();
            let client_thread = thread::spawn(move || {
                client_tx.request(&Query("Testing".to_string()))
                    .and_then(|resp| {
                                  assert_eq!(resp.0, "Testing".to_string());
                                  Ok(())
                              })
                    .wait()
                    .unwrap();
            });

            while root.step() {}

            let _ = client_thread.join();
        })
                .unwrap();
    }
}
