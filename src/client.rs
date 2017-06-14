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

pub struct KeeperStreamBuilder<'a, Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    keeper_name: &'a str,
    coord: &'a Coordinator,
    _query_type: PhantomData<Q>,
    _response_type: PhantomData<R>,
}

impl<'a, Q, R> KeeperStreamBuilder<'a, Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    pub fn new(keeper_name: &'a str, coord: &'a Coordinator) -> Self {
        KeeperStreamBuilder {
            keeper_name: keeper_name,
            coord: coord,
            _query_type: PhantomData,
            _response_type: PhantomData,
        }
    }

    pub fn query<'b>(&'b mut self, query: Q) -> Result<KeeperStream<Q, R>, KeeperLookupError> {
        let messenger = get_messenger_of_keeper(self.keeper_name, self.coord)?;
        messenger.send_message(KeeperQuery::Query(query))?;
        Ok(KeeperStream {
            messenger: Some(messenger),
        })
    }

    pub fn state<'b>(&'b mut self,
                     query: StateRequest)
                     -> Result<KeeperStream<Q, R>, KeeperLookupError> {
        let messenger = get_messenger_of_keeper(self.keeper_name, self.coord)?;
        messenger.send_message(KeeperQuery::StateRq(query))?;
        Ok(KeeperStream {
            messenger: Some(messenger),
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

pub struct KeeperStream<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    messenger: Option<Messenger<KeeperResponse<R>, KeeperQuery<Q>>>,
}

impl<Q, R> Stream for KeeperStream<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    type Item = R;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let value = match self.messenger.as_mut() {
            Some(msgr) => try_ready!(msgr.poll()),
            None => return Ok(Async::Ready(None)),
        };

        Ok(match value {
            Some(resp) => match resp {
                KeeperResponse::Response(r) => Async::Ready(Some(r)),
                KeeperResponse::BatchEnd => {
                    // TODO: here we could increment progress tracking or something
                    Async::NotReady
                },
                KeeperResponse::ConnectionEnd => {
                    drop(self.messenger.take());
                    Async::Ready(None)
                }
            },
            None => Async::Ready(None),
        })
    }
}

impl<Q, R> IntoIterator for KeeperStream<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    type Item = R;
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
