use std::io::{Error, Result, ErrorKind};
use std::net::ToSocketAddrs;
use std::any::Any;
use std::iter::repeat;

use futures::Future;
use futures::stream::Stream;
use abomonation::Abomonation;

use network::Network;
use network::reqrep::{Outgoing, Response};
use network::message::abomonate::NonStatic;

use pubsub::subscriber::CollectionSubscriber;

use coordinator::requests::*;
use model::*;

pub struct Submitter {
    tx: Outgoing,
    network: Network,
}

impl Submitter {
    pub fn new<E: ToSocketAddrs>(network: &Network, addr: E) -> Result<Self> {
        let (tx, _) = network.client(addr)?;
        Ok(Submitter {
            tx: tx,
            network: network.clone(),
        })
    }

    pub fn submit<N>(&self,
                     query: QueryProgram,
                     name: N,
                     placement: Placement)
                     -> Response<Submission>
        where N: Into<Option<String>>
    {
        let submission = Submission {
            query: query,
            name: name.into(),
            placement: placement,
        };

        self.tx.request(&submission)
    }

    fn lookup(&self, name: &str) -> Result<Topic> {
        self.tx
            .request(&Lookup { name: name.into() })
            .map_err(|e| match e {
                Ok(()) => Error::new(ErrorKind::Other, "topic not found"),
                Err(err) => err,
            })
            .wait()
    }

    fn get_collection<D>(&self, name: &str) -> Result<Vec<D>>
        where D: Abomonation + Any + Clone + NonStatic
    {
        let topic = self.lookup(name)?;
        assert_eq!(topic.schema, TopicSchema::Collection(TopicType::of::<D>()));

        let sub = CollectionSubscriber::<D>::connect(&topic, &self.network)?;

        match sub.into_future().wait() {
            Ok((Some(vec), _)) => {
                Ok(vec.into_iter()
                    .flat_map(|(item, n)| repeat(item).take(n as usize))
                    .collect())
            }
            Ok((None, _)) => {
                Err(Error::new(ErrorKind::Other, "subscriber stopped unexpectedly"))
            }
            Err((err, _)) => Err(err),
        }
    }

    pub fn topics(&self) -> Result<Vec<Topic>> {
        self.get_collection("$topics")
    }

    pub fn executors(&self) -> Result<Vec<Executor>> {
        self.get_collection("$executors")
    }

    pub fn queries(&self) -> Result<Vec<Query>> {
        self.get_collection("$queries")
    }

    pub fn publications(&self) -> Result<Vec<Publication>> {
        self.get_collection("$publications")
    }

    pub fn subscriptions(&self) -> Result<Vec<Subscription>> {
        self.get_collection("$subscriptions")
    }
}
