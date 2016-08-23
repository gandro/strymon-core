use std::sync::mpsc;
use std::ops::RangeFrom;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::thread;

use query::{QueryId, QueryConfig};
use executor::{ExecutorId, ExecutorType};
use messaging::response::Promise;

pub use coordinator::request::*;

use self::pending::*;
use self::query::*;
use self::executor::*;

mod pending;
mod executor;
mod query;

#[derive(Clone)]
pub struct CatalogRef(mpsc::Sender<Message>);

impl CatalogRef {
    pub fn send(&self, msg: Message) {
        self.0.send(msg).expect("invalid catalog ref")
    }
}

pub enum Message {
    Submission(Submission, SubmissionPromise),
}

pub struct Catalog {
    pending: BTreeMap<QueryId, Pending>,
    queries: BTreeMap<QueryId, Query>,

    executors: Executors,

    query_id: Generator<QueryId>,
    requests: mpsc::Receiver<Message>,
}

impl Catalog {
    pub fn new() -> (CatalogRef, Catalog) {
        let (tx, rx) = mpsc::channel();

        let catalog_ref = CatalogRef(tx);
        let catalog = Catalog {
            pending: BTreeMap::new(),
            queries: BTreeMap::new(),
            executors: Executors::new(),

            query_id: Generator::new(),
            requests: rx,
        };
        
        (catalog_ref, catalog)
    }

    pub fn run(&mut self) {
        while let Ok(request) = self.requests.recv() {
            self.process(request);
        }
    }
    
    pub fn detach(mut self) {
        thread::spawn(move || self.run());
    }

    pub fn process(&mut self, request: Message) {
        use self::Message::*;
        match request {
            Submission(submission, promise) => self.submission(submission, promise),
        };
    }

    pub fn submission(&mut self,
                      submission: Submission,
                      promise: Promise<QueryId, SubmissionError>) {
        let id = self.query_id.generate();
        let config = submission.config;
        
        // find suiting executors
        let selected = self.executors.select(config.binary, config.num_executors);
        
        // check if we have enough executors of the right type
        let executors = match selected {
            Some(ref executors) if executors.len() < config.num_executors => {
                return promise.failed(SubmissionError::NotEnoughExecutors);
            }
            None => {
                return promise.failed(SubmissionError::NoExecutorsForType);
            }
            Some(executors) => executors,
        };
               
        // ask executors to spawn a new query
        for executor in executors {
            executor.spawn(id, &config);
        }

        // install pending query
        let pending = Pending::new(id, config, promise);

        // TODO maybe we should add a timeout for the pending query..?!
        self.pending.insert(id, pending);
    }
}

struct Generator<T> {
    generator: RangeFrom<u64>,
    marker: PhantomData<T>,
}

impl<T: From<u64>> Generator<T> {
    fn new() -> Self {
        Generator {
            generator: 0..,
            marker: PhantomData,
        }
    }

    fn generate(&mut self) -> T {
        From::from(self.generator.next().unwrap())
    }
}
