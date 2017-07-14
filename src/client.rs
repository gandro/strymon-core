//! Client library for querying Keeper state.
//!
//! Each Keeper exposes an interface for a read-only access to its state. At the minimum it allows
//! clients to request a "dump" of its state, subscribe to updates to the state or both at once.
//! Those queries can be performed calling `state` method on `KeeperConnection` object with
//! `StateRequest` object specifying the type of request.
//! In addition, some Keepers can implement adhoc queries to be called against their state. Those
//! queries are specific to their Keeper. For the adhoc queries one needs to call the `query`
//! method on the `KeeperConnection` object.
//! Note, that a Keeper's client needs to be compiled together with the Keeper typically, until we
//! stop using Abomonation.
//!
//! `KeeperStream` object represents a stream of tuples produced by a Keeper in response to a query
//! or a state request. In case of a single state dump or an adhoc query the stream is a one-off
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

use abomonation::Abomonation;
use futures::{Async, Poll};
use futures::stream::{Stream, Wait};
use timely_system::network::Network;
use timely_system::network::message::abomonate::NonStatic;
use timely_system::query::Coordinator;
use timely_system::query::keepers::KeeperLookupError;

use keeper::messenger::Messenger;
use model::{KeeperQuery, KeeperResponse, StateRequest};

/// Object representing a connection to a Keeper.
/// It is parametrized with the type of query that the Keeper accepts (`Q`) and the type of
/// response that the Keeper returns (`R`). They should be explicitly provided when a new object is
/// created.
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
           })
    }

    /// Send an adhoc query.
    pub fn query<'b>(&'b mut self, query: Q) -> Result<KeeperStream<Q, R, R>, KeeperLookupError> {
        let messenger = get_messenger_of_keeper(self.keeper_name, self.coord)?;
        messenger.send_message(KeeperQuery::Query(query))?;
        Ok(KeeperStream {
               messenger: Some(messenger),
               convert_element_fn: convert_element_regular,
           })
    }
}

fn get_messenger_of_keeper<Q, R>
    (keeper_name: &str,
     coord: &Coordinator)
     -> Result<Messenger<KeeperResponse<R>, KeeperQuery<Q>>, KeeperLookupError>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    let keeper = coord.lookup_keeper(keeper_name)?;
    let network = Network::init()?;
    let (tx, rx) = network.connect((&keeper.addr.0[..], keeper.addr.1))?;
    let messenger = Messenger::new(tx, rx);
    Ok(messenger)
}

fn convert_element_regular<R>(element: KeeperResponse<R>) -> Async<Option<R>>
    where R: Abomonation + Any + Clone + Send + NonStatic
{
    match element {
        KeeperResponse::Response(r) => Async::Ready(Some(r)),
        KeeperResponse::BatchEnd => Async::NotReady,
        KeeperResponse::ConnectionEnd => Async::Ready(None),
    }
}

fn convert_element_with_batch_markers<R>(element: KeeperResponse<R>) -> Async<Option<Element<R>>>
    where R: Abomonation + Any + Clone + Send + NonStatic
{
    match element {
        KeeperResponse::Response(r) => Async::Ready(Some(Element::Value(r))),
        KeeperResponse::BatchEnd => Async::Ready(Some(Element::BatchEnd)),
        KeeperResponse::ConnectionEnd => Async::Ready(None),
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum Element<O>
    where O: Abomonation + Any + Clone + Send + NonStatic
{
    Value(O),
    BatchEnd,
}

pub struct KeeperStream<Q, R, O>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic,
          O: Abomonation + Any + Clone + Send + NonStatic
{
    messenger: Option<Messenger<KeeperResponse<R>, KeeperQuery<Q>>>,
    convert_element_fn: fn(KeeperResponse<R>) -> Async<Option<O>>,
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
            let value = match self.messenger.as_mut() {
                Some(msgr) => try_ready!(msgr.poll()),
                None => return Ok(Async::Ready(None)),
            };

            let ret = match value {
                Some(resp) => (self.convert_element_fn)(resp),
                None => Async::Ready(None),
            };
            match ret {
                Async::Ready(None) => drop(self.messenger.take()),
                Async::NotReady => continue,
                _ => (),
            };
            return Ok(ret);
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
