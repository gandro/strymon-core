// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;

use timely::ExchangeData;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::{Capture, Exchange};
use timely::dataflow::operators::capture::{Event, EventPusher};

use serde::ser::Serialize;
use futures::Future;
use typename::TypeName;

use strymon_model::{Topic, TopicId, TopicType, TopicSchema};
use strymon_rpc::coordinator::*;

use Coordinator;
use protocol::RemoteTimestamp;
use publisher::Publisher;

#[derive(Debug)]
pub enum PublicationError {
    TopicAlreadyExists,
    TopicNotFound,
    AuthenticationFailure,
    TypeIdMismatch,
    IoError(io::Error),
}

impl From<PublishError> for PublicationError {
    fn from(err: PublishError) -> Self {
        match err {
            PublishError::TopicAlreadyExists => PublicationError::TopicAlreadyExists,
            err => panic!("failed to publish: {:?}", err),
        }
    }
}

impl From<io::Error> for PublicationError {
    fn from(err: io::Error) -> Self {
        PublicationError::IoError(err)
    }
}

impl From<UnpublishError> for PublicationError {
    fn from(err: UnpublishError) -> Self {
        match err {
            UnpublishError::InvalidTopicId => PublicationError::TopicNotFound,
            UnpublishError::AuthenticationFailure => {
                PublicationError::AuthenticationFailure
            }
        }
    }
}

impl<T, E> From<Result<T, E>> for PublicationError
    where T: Into<PublicationError>,
          E: Into<PublicationError>
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Partition {
    PerWorker,
    Merge,
}

const PUBLISH_WORKER_ID: u64 = 0;

impl Partition {
    fn name(&self, name: &str, worker_id: u64) -> Option<String> {
        match *self {
            Partition::PerWorker => Some(format!("{}.{}", name, worker_id)),
            Partition::Merge if worker_id == PUBLISH_WORKER_ID => {
                Some(String::from(name))
            }
            _ => None,
        }
    }
}

enum Publication<T, D> {
    // publication exists on local worker
    Local(Topic, Coordinator, Publisher<T, D>),
    // the publication is on a remote worker
    Remote,
}

impl<T, D> EventPusher<T, D> for Publication<T, D>
    where T: RemoteTimestamp, D: ExchangeData + Serialize
{
    fn push(&mut self, event: Event<T, D>) {
        if let Publication::Local(_, _, ref mut publisher) = *self {
            publisher.push(event)
        }
    }
}

impl<T, D> Drop for Publication<T, D> {
    fn drop(&mut self) {
        if let Publication::Local(ref topic, ref coord, _) = *self {
            if let Err(err) = coord.unpublish(topic.id) {
                warn!("failed to unpublish: {:?}", err)
            }
        }
    }
}

impl Coordinator {
    fn publish_request(&self,
                       name: String,
                       schema: TopicSchema,
                       addr: (String, u16))
                       -> Result<Topic, PublicationError>
    {
        self.tx
            .request(&Publish {
                name: name,
                token: self.token,
                schema: schema,
                addr: addr,
            })
            .map_err(PublicationError::from)
            .wait()
    }

    pub fn publish<S, D>(&self,
                         name: &str,
                         stream: &Stream<S, D>,
                         partition: Partition)
                         -> Result<(), PublicationError>
        where D: ExchangeData + Serialize + TypeName,
              S: Scope,
              S::Timestamp: RemoteTimestamp,
              <S::Timestamp as RemoteTimestamp>::Remote: TypeName
    {
        // if we have an assigned topic name, we need to create a publisher
        let worker_id = stream.scope().index() as u64;
        let publication = if let Some(name) = partition.name(name, worker_id) {
            let (addr, publisher) = Publisher::<S::Timestamp, D>::new(&self.network)?;
            let item = TopicType::of::<D>();
            let time = TopicType::of::<<S::Timestamp as RemoteTimestamp>::Remote>();
            let schema = TopicSchema::Stream(item, time);

            // announce the publication to the coordinator
            let topic = self.publish_request(name, schema, addr)?;
            Publication::Local(topic, self.clone(), publisher)
        } else {
            Publication::Remote
        };

        if let Partition::Merge = partition {
            stream.exchange(|_| PUBLISH_WORKER_ID).capture_into(publication);
        } else {
            stream.capture_into(publication);
        };

        Ok(())
    }

    fn unpublish(&self, topic: TopicId) -> Result<(), PublicationError> {
        self.tx
            .request(&Unpublish {
                topic: topic,
                token: self.token,
            })
            .map_err(PublicationError::from)
            .wait()
    }
}
