//! Client library for querying Keeper state.
//!
//! Each Keeper exposes an interface for a read-only access to its state. At the minimum it allows
//! clients to request a "dump" of its state, subscribe to updates to the state or both at once.
//! Those queries can be performed calling `state` method on `KeeperConnection` object with
//! `StateRequest` object specifying the type of request.
//! In addition, some Keepers can implement point queries to be called against their state. Those
//! queries are specific to their Keeper. For the point queries one needs to call the `query`
//! method on the `KeeperConnection` object.
//! Note, that a Keeper's client needs to be compiled together with the Keeper typically, until we
//! stop using Abomonation.
//!
//! `KeeperStream` object represents a stream of tuples produced by a Keeper in response to a query
//! or a state request. In case of a single state dump or a point query the stream is a one-off
//! batch of tuples. When a client subscribes to updates though, the stream is infinite - each
//! time the Keeper's state is updated some new tuples arrive on that stream. If the client wants
//! to be able to distinguish between the batches (eg. in order to advance the progress tracking)
//! they should use `state_with_batch_markers` method.
//! `KeeperStream` implements both Iterator and futures' Stream so it can be iterated over in the
//! fashion that suits the user the best.
//!
use std::any::Any;
use std::io;
use std::marker::PhantomData;
use std::collections::VecDeque;
use std::fmt;

use abomonation::Abomonation;
use futures::{Async, Poll};
use futures::stream::{Stream, Wait};
use timely_system::network::Network;
use timely_system::network::message::abomonate::NonStatic;
use timely_system::query::Coordinator;
use timely_system::query::keepers::KeeperLookupError;

use keeper::messenger::Messenger;
use model::{KeeperQuery, KeeperResponse, StateRequest};

/// Object representing a connection to a Keeper. It is parametrized with the type of query that
/// the Keeper accepts (`Q`) and the type of response that the Keeper returns (`R`). They should be
/// explicitly provided when a new object is created. If the Keeper doesn't support point queries
/// at all, model::EmptyQT should be used as the query type.
///
/// Data sent back from Keeper is always in a form of a stream. The stream might be infinite if
/// client subscribes to the updates, or finite in other cases. In case of infinite stream you may
/// want to use `state_with_batch_markers` function---it exposes information about the ends of
/// batches sent from Keeper that could be used for advancing progress tracking information.
///
/// For now, the Abomonation library is used for the serialization but it will be changed in
/// future. This version of the code doesn't check if the parameters specified are valid and it
/// just hangs on communication with the Keeper if invalid ones are used.
pub struct KeeperConnection<'a, Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    keeper_name: &'a str,
    coord: &'a Coordinator,
    _query_type: PhantomData<Q>,
    _response_type: PhantomData<R>,
}

impl<'a, Q, R> KeeperConnection<'a, Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    pub fn new(keeper_name: &'a str, coord: &'a Coordinator) -> Self {
        KeeperConnection {
            keeper_name: keeper_name,
            coord: coord,
            _query_type: PhantomData,
            _response_type: PhantomData,
        }
    }

    pub fn state<'b>(&'b mut self,
                     query: StateRequest)
                     -> Result<KeeperStream<Q, R, R>, KeeperLookupError> {
        let messenger = get_messenger_of_keeper(self.keeper_name, self.coord)?;
        messenger.send_message(KeeperQuery::StateRq(query))?;
        Ok(KeeperStream {
               messenger: Some(messenger),
               convert_element_fn: convert_element_regular,
               buffer: VecDeque::new(),
           })
    }

    pub fn state_with_batch_markers<'b>
        (&'b mut self,
         query: StateRequest)
         -> Result<KeeperStream<Q, R, Element<R>>, KeeperLookupError> {
        let messenger = get_messenger_of_keeper(self.keeper_name, self.coord)?;
        messenger.send_message(KeeperQuery::StateRq(query))?;
        Ok(KeeperStream {
               messenger: Some(messenger),
               convert_element_fn: convert_element_with_batch_markers,
               buffer: VecDeque::new(),
           })
    }

    /// Send a point query.
    pub fn query<'b>(&'b mut self, query: Q) -> Result<KeeperStream<Q, R, R>, KeeperLookupError> {
        let messenger = get_messenger_of_keeper(self.keeper_name, self.coord)?;
        messenger.send_message(KeeperQuery::Query(query))?;
        Ok(KeeperStream {
               messenger: Some(messenger),
               convert_element_fn: convert_element_regular,
               buffer: VecDeque::new(),
           })
    }
}

fn get_messenger_of_keeper<Q, R>
    (keeper_name: &str,
     coord: &Coordinator)
     -> Result<Messenger<Vec<KeeperResponse<R>>, KeeperQuery<Q>>, KeeperLookupError>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    let (addr, port) = coord.get_keeper_address(keeper_name)?;
    let network = Network::init()?;
    let (tx, rx) = network.connect((&addr[..], port))?;
    let messenger = Messenger::new(tx, rx);
    Ok(messenger)
}

/// Returns false if we reached ConnectionEnd marker.
fn convert_element_regular<R>(batch: Vec<KeeperResponse<R>>, out_buff: &mut VecDeque<R>) -> bool
    where R: Abomonation + Any + Clone + Send + NonStatic
{
    for element in batch {
        match element {
            KeeperResponse::Response(r) => out_buff.push_back(r),
            KeeperResponse::BatchEnd => (),
            KeeperResponse::ConnectionEnd => return false,
        }
    }
    true
}

/// Returns false if we reached ConnectionEnd marker.
fn convert_element_with_batch_markers<R>(batch: Vec<KeeperResponse<R>>,
                                         out_buff: &mut VecDeque<Element<R>>)
                                         -> bool
    where R: Abomonation + Any + Clone + Send + NonStatic
{
    for element in batch {
        match element {
            KeeperResponse::Response(r) => out_buff.push_back(Element::Value(r)),
            KeeperResponse::BatchEnd => out_buff.push_back(Element::BatchEnd),
            KeeperResponse::ConnectionEnd => return false,
        }
    }
    true
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum Element<O>
    where O: Abomonation + Any + Clone + Send + NonStatic
{
    Value(O),
    BatchEnd,
}

/// Object representing a stream of tuples produced by a Keeper in response to a query from client.
///
/// It is iterable and implements `Stream` from futures-rs.
pub struct KeeperStream<Q, R, O>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic,
          O: Abomonation + Any + Clone + Send + NonStatic
{
    messenger: Option<Messenger<Vec<KeeperResponse<R>>, KeeperQuery<Q>>>,
    convert_element_fn: fn(Vec<KeeperResponse<R>>, &mut VecDeque<O>) -> bool,
    buffer: VecDeque<O>,
}

impl<Q, R, O> Stream for KeeperStream<Q, R, O>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic,
          O: Abomonation + Any + Clone + Send + NonStatic
{
    type Item = O;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let mut polled_smth = false;
            let mut connection_finished = false;
            match self.messenger.as_mut() {
                Some(msgr) => {
                    match msgr.poll()? {
                        Async::Ready(Some(batch)) => {
                            polled_smth = true;
                            connection_finished = !(self.convert_element_fn)(batch,
                                                                             &mut self.buffer);
                        }
                        Async::Ready(None) => connection_finished = true,
                        Async::NotReady => (),
                    }
                }
                None => (),
            };
            if connection_finished {
                drop(self.messenger.take());
            }
            return Ok(match self.buffer.pop_front() {
                          Some(resp) => Async::Ready(Some(resp)),
                          None => {
                              if polled_smth {
                                  continue;
                              } else {
                                  if self.messenger.is_some() {
                                      Async::NotReady
                                  } else {
                                      Async::Ready(None)
                                  }
                              }
                          }
                      });
        }
    }
}

impl<Q, R, O> IntoIterator for KeeperStream<Q, R, O>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic,
          O: Abomonation + Any + Clone + Send + NonStatic
{
    type Item = O;
    type IntoIter = IntoIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { inner: self.wait() }
    }
}

pub struct IntoIter<I> {
    inner: Wait<I>,
}

impl<S: Stream> Iterator for IntoIter<S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().and_then(Result::ok)
    }
}

impl<Q, R, O> fmt::Debug for KeeperStream<Q, R, O>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic,
          O: Abomonation + Any + Clone + Send + NonStatic
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KeeperStream")
    }
}
