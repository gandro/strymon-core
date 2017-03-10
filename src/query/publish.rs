use std::io::Error as IoError;

use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::channels::Content;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely_communication::{Allocate, Pull, Push};
use futures::Future;

use query::Coordinator;
use coordinator::requests::*;
use model::{Topic, TopicId, TopicType, TopicSchema};
use network::message::abomonate::NonStatic;
use pubsub::publisher::timely::TimelyPublisher;
use pubsub::publisher::collection::CollectionPublisher;

#[derive(Debug)]
pub enum PublicationError {
    TopicAlreadyExists,
    TopicNotFound,
    AuthenticationFailure,
    TypeIdMismatch,
    IoError(IoError),
}

impl From<PublishError> for PublicationError {
    fn from(err: PublishError) -> Self {
        match err {
            PublishError::TopicAlreadyExists => PublicationError::TopicAlreadyExists,
            err => panic!("failed to publish: {:?}", err),
        }
    }
}

impl From<IoError> for PublicationError {
    fn from(err: IoError) -> Self {
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

impl<T: Timestamp, D: Data> ParallelizationContract<T, D> for Partition {
    fn connect<A: Allocate>
        (self,
         allocator: &mut A,
         identifier: usize)
         -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        match self {
            Partition::PerWorker => Pipeline.connect(allocator, identifier),
            Partition::Merge => {
                Exchange::new(|_| PUBLISH_WORKER_ID).connect(allocator, identifier)
            }
        }
    }
}

impl Partition {
    fn name(&self, name: &str, worker_id: u64) -> Option<String> {
        match *self {
            Partition::PerWorker => Some(format!("{}.{}", name, worker_id)),
            Partition::Merge if (worker_id == PUBLISH_WORKER_ID) => {
                Some(String::from(name))
            }
            _ => None,
        }
    }
}

struct Publication {
    topic: Topic,
    coord: Coordinator,
}

impl Drop for Publication {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unpublish(self.topic.id) {
            warn!("failed to unpublish: {:?}", err)
        }
    }
}

impl Coordinator {
    fn publish_request(&self,
                       name: String,
                       schema: TopicSchema,
                       addr: (String, u16))
                       -> Result<Publication, PublicationError> {
        let topic = self.tx
            .request(&Publish {
                name: name,
                token: self.token,
                schema: schema,
                addr: addr,
            })
            .map_err(PublicationError::from)
            .wait()?;

        Ok(Publication {
            topic: topic,
            coord: self.clone(),
        })
    }

    pub fn publish<S, D>(&self,
                         name: &str,
                         stream: &Stream<S, D>,
                         partition: Partition)
                         -> Result<Stream<S, D>, PublicationError>
        where D: Data + NonStatic,
              S: Scope,
              S::Timestamp: NonStatic
    {
        let worker_id = stream.scope().index() as u64;
        let name = partition.name(name, worker_id);

        let (addr, mut publisher) = if name.is_some() {
            let (addr, publisher) =
                TimelyPublisher::<S::Timestamp, D>::new(&self.network)?;
            (Some(addr), Some(publisher))
        } else {
            (None, None)
        };

        let publication = if name.is_some() {
            // local worker hosts a publication
            let item = TopicType::of::<D>();
            let time = TopicType::of::<S::Timestamp>();
            let schema = TopicSchema::Stream(item, time);

            Some(self.publish_request(name.unwrap(), schema, addr.unwrap())?)
        } else {
            None
        };

        let output = stream.unary_notify(partition,
                                         "timelypublisher",
                                         Vec::new(),
                                         move |input, output, notif| {
            // ensure publication handle is moved into the closure/operator
            let ref _guard = publication;

            // publish data on input
            let frontier = notif.frontier(0);
            input.for_each(|time, data| {
                if let Some(ref mut publisher) = publisher {
                    publisher.publish(frontier, &time, data).unwrap();
                }
                output.session(&time).give_content(data);
            });
        });

        Ok(output)
    }

    pub fn publish_collection<S, D>(&self,
                                    name: &str,
                                    stream: &Stream<S, (D, i32)>,
                                    partition: Partition)
                                    -> Result<Stream<S, (D, i32)>, PublicationError>
        where D: Data + Eq + NonStatic,
              S: Scope
    {
        let worker_id = stream.scope().index() as u64;
        let name = partition.name(name, worker_id);

        let (addr, mut mutator, mut publisher) = if name.is_some() {
            let (addr, mutator, publisher) =
                CollectionPublisher::<D>::new(&self.network)?;
            (Some(addr), Some(mutator), Some(publisher.spawn()))
        } else {
            (None, None, None)
        };

        let publication = if name.is_some() {
            // local worker hosts a publication
            let item = TopicType::of::<D>();
            let schema = TopicSchema::Collection(item);
            Some(self.publish_request(name.unwrap(), schema, addr.unwrap())?)
        } else {
            None
        };

        let output =
            stream.unary_stream(partition, "collectionpublisher", move |input, output| {
                // ensure publication handle is moved into the closure/operator
                let ref _guard = publication;

                // publication logic
                input.for_each(|time, data| {
                    if let Some(ref mut mutator) = mutator {
                        mutator.update_from(data.clone().into_typed());
                    }
                    output.session(&time).give_content(data);
                });

                // ensure publisher future is polled
                if let Some(ref mut publisher) = publisher {
                    publisher.poll().unwrap();
                }
            });

        Ok(output)
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
