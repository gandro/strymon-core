use std::io;
use std::any::Any;

use abomonation::Abomonation;
use futures::Poll;
use futures::stream::{Stream, Wait};
use timely_system::query::Coordinator;
use timely_system::network::Network;
use timely_system::query::keepers::KeeperLookupError;
use timely_system::network::message::abomonate::NonStatic;

use keeper::messenger::Messenger;

/// Supposed to be used as something you can iterate over and send data it produces to input.
pub struct KeeperQuery<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    messenger: Messenger<Q, R>,
}

impl<Q, R> KeeperQuery<Q, R> 
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    pub fn new(query: &R, keeper_name: &str, coord: &Coordinator) -> Result<Self, KeeperLookupError> {
        let keeper = coord.lookup_keeper(keeper_name)?;
        let network = Network::init()?;
        let (tx, rx) = network.connect((&keeper.addr.0[..], keeper.addr.1))?;
        let messenger = Messenger::new(tx, rx);
        messenger.send_message(query.clone())?;
        Ok(KeeperQuery {
            messenger: messenger,
        })
    }
}

impl<Q, R> Stream for KeeperQuery<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    type Item = Q;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.messenger.poll()
    }
}

impl<Q, R> IntoIterator for KeeperQuery<Q, R>
    where Q: Abomonation + Any + Clone + NonStatic,
          R: Abomonation + Any + Clone + Send + NonStatic
{
    type Item = Q;
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
