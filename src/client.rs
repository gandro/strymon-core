use std::io;

use futures::{Async, Poll};
use futures::stream::{Stream, Wait};
use timely_system::query::Coordinator;
use timely_system::network::Network;
use timely_system::query::keepers::KeeperLookupError;

use core::messenger::Messenger;
use core::{Query, ResponseTuple};

/// Supposed to be used as something you can iterate over and send data it produces to input.
pub struct KeeperQuery {
    messenger: Messenger<ResponseTuple, Query>,
}

impl KeeperQuery {
    pub fn new(query: &str, keeper_name: &str, coord: &Coordinator) -> Result<Self, KeeperLookupError> {
        let keeper = coord.lookup_keeper(keeper_name)?;
        let network = Network::init()?;
        let (tx, rx) = network.connect((&keeper.addr.0[..], keeper.addr.1))?;
        let messenger = Messenger::new(tx, rx);
        messenger.send_message(Query::new(query))?;
        Ok(KeeperQuery {
            messenger: messenger,
        })
    }
}

impl Stream for KeeperQuery {
    type Item = String;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(match try_ready!(self.messenger.poll()) {
            Some(ResponseTuple { text }) => Some(text),
            None => None,
        }))
    }
}

impl IntoIterator for KeeperQuery {
    type Item = String;
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
