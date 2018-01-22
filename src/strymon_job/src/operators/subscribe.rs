// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;

use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;

use futures::{Future, Poll};
use futures::stream::{Stream, Wait};

use serde::de::DeserializeOwned;

use strymon_rpc::coordinator::*;
use strymon_model::{Topic, TopicId};

use Coordinator;
use subscriber::Subscriber;
use protocol::RemoteTimestamp;

pub struct Subscription<T: Timestamp, D> {
    sub: Subscriber<T, D>,
    topic: Topic,
    coord: Coordinator,
}

impl<T, D> Stream for Subscription<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
    T: ::std::fmt::Debug, D: ::std::fmt::Debug,
{
    type Item = (Capability<T>, Vec<D>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.sub.poll()
    }
}

#[derive(Debug)]
pub struct IntoIter<I> {
    inner: Wait<I>,
}

impl<T, D> IntoIterator for Subscription<T, D>
    where T: RemoteTimestamp,
          D: DeserializeOwned,
          T: ::std::fmt::Debug, D: ::std::fmt::Debug,
{
    type Item = (Capability<T>, Vec<D>);
    type IntoIter = IntoIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { inner: self.wait() }
    }
}

impl<S: Stream> Iterator for IntoIter<S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().and_then(Result::ok)
    }
}

impl<T: Timestamp, D> Drop for Subscription<T, D> {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unsubscribe(self.topic.id) {
            error!("failed to unsubscribe: {:?}", err)
        }
    }
}

#[derive(Debug)]
pub enum SubscriptionError {
    TopicNotFound,
    TypeIdMismatch,
    AuthenticationFailure,
    IoError(io::Error),
}

impl From<SubscribeError> for SubscriptionError {
    fn from(err: SubscribeError) -> Self {
        match err {
            SubscribeError::TopicNotFound => SubscriptionError::TopicNotFound,
            SubscribeError::AuthenticationFailure => {
                SubscriptionError::AuthenticationFailure
            }
        }
    }
}

impl From<UnsubscribeError> for SubscriptionError {
    fn from(err: UnsubscribeError) -> Self {
        match err {
            UnsubscribeError::InvalidTopicId => SubscriptionError::TopicNotFound,
            UnsubscribeError::AuthenticationFailure => {
                SubscriptionError::AuthenticationFailure
            }
        }
    }
}

impl From<io::Error> for SubscriptionError {
    fn from(err: io::Error) -> Self {
        SubscriptionError::IoError(err)
    }
}

impl<T, E> From<Result<T, E>> for SubscriptionError
    where T: Into<SubscriptionError>,
          E: Into<SubscriptionError>
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

impl Coordinator {
    fn unsubscribe(&self, topic: TopicId) -> Result<(), SubscriptionError> {
        self.tx
            .request(&Unsubscribe {
                topic: topic,
                token: self.token,
            })
            .map_err(SubscriptionError::from)
            .wait()
    }

    pub fn subscribe<T, D>(&self,
                    name: &str,
                    root: Capability<T>,
                    blocking: bool)
                    -> Result<Subscription<T, D>, SubscriptionError>
        where T: RemoteTimestamp,
              D: DeserializeOwned,
              T: ::std::fmt::Debug, D: ::std::fmt::Debug,
    {
        let name = name.to_string();
        let coord = self.clone();
        self.tx
            .request(&Subscribe {
                name: name,
                token: self.token,
                blocking: blocking,
            })
            .map_err(SubscriptionError::from)
            .and_then(move |topic| {
                if !topic.schema.is_stream() {
                    return Err(SubscriptionError::TypeIdMismatch);
                }

                let socket = self.network.connect((&*topic.addr.0, topic.addr.1))?;
                let sub = Subscriber::<T, D>::new(socket, root);
                Ok(Subscription {
                    sub: sub,
                    topic: topic,
                    coord: coord,
                })
            })
            .wait()
    }
}
