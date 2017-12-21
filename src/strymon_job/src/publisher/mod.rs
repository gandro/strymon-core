use std::io;
use std::thread;
use std::marker::PhantomData;

use slab::Slab;
use timely::ExchangeData;
use timely::progress::timestamp::Timestamp;
use timely::dataflow::operators::capture::event::Event as TimelyEvent;
use tokio_core::reactor::{Core, Handle};

use strymon_communication::Network;
use strymon_communication::transport::{Listener, Sender, Receiver};

use futures::future::Future;
use futures::stream::{self, Stream};
use futures::unsync;

use self::progress::ProgressState;
use self::server::{PubSubServer, SubscriberEvent};
use self::sink::{EventSink, EventStream};

mod sink;
mod progress;
mod server;

enum Event<T, D> {
    Timely(TimelyEvent<T, D>),
    Accepted((Sender, Receiver)),
    Disconnected(SubscriberId),
    ShutdownRequested,
}

type SubscriberId = usize;
type FeedbackSender<T, D> = unsync::mpsc::UnboundedSender<Event<T, D>>;

pub struct PublisherServer<T: Timestamp, D> {
    progress: ProgressState<T>,
    subscribers: Slab<Sender>,
    feedback: FeedbackSender<T, D>,
    executor: Handle,
}

impl<T: Timestamp, D: ExchangeData> PublisherServer<T, D> {
    fn new(feedback: FeedbackSender<T, D>, executor: Handle) -> Self {
        PublisherServer {
            progress: ProgressState::init(),
            subscribers: Slab::new(),
            feedback: feedback,
            executor: executor,
        }
    }

    fn timely_event(&mut self, event: TimelyEvent<T, D>) {

    }

    fn accepted(&mut self, (tx, rx): (Sender, Receiver)) {
        let id = self.subscribers.insert(tx);

    }

    fn disconnected(&mut self, subscriber_id: SubscriberId) {
        
    }

    fn shutdown(&mut self) {

    }
}

pub type Addr = (String, u16);

struct Publisher<T, D> {
    pub sink: EventSink<T, D>,
    pub addr: Addr,
    pub thread: Thread,
}

impl<T, D> Publisher<T, D> where T: Timestamp, D: ExchangeData {
    fn new(network: &Network) -> io::Result<Self> {
        // the queue between the Timely operator and this publisher thread
        let (timely_sink, timely) = sink::pair();

        // the network socket on which subscribers are accepted
        let listener = network.listen(None)?;
        let addr = {
            let (host, port) = listener.external_addr();
            (String::from(host), port)
        };

        // main event loop of the publisher thread
        let handle = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();

            let (feedback_sink, feedback) = unsync::mpsc::unbounded();
            // forward networking and timely events. note that once the timely
            // producer goes away, we want to drain any subscriber queues and
            // then shut down
            let accept = listener.map(Event::Accepted);
            let timely = timely.map(Event::Timely)
                .map_err(|_| unreachable!("receiver should never fail"))
                .chain(stream::once(Ok(Event::ShutdownRequested)));
            let events = feedback
                .map_err(|_| unreachable!("receiver should never fail"));

            // contains subscriber queue handles, progress logic etc
            let mut publisher = PublisherServer::new(feedback_sink, handle);

            // main event loop
            let events = events.select(timely).select(accept);
            let event_loop = events.for_each(move |event| {
                match event {
                    Event::Timely(ev) => publisher.timely_event(ev),
                    Event::Accepted(conn) => publisher.accepted(conn),
                    Event::Disconnected(id) => publisher.disconnected(id),
                    Event::ShutdownRequested => publisher.shutdown(),
                };

                Ok(())
            });

            if let Err(err) = core.run(event_loop) {
                error!("publisher event loop error: {}", err);
            }
        });

        Ok(Publisher {
            sink: timely_sink,
            addr: addr,
            thread: Thread::new(handle),
        })
    }
}

type ThreadHandle = thread::JoinHandle<()>;

pub struct Thread(Option<ThreadHandle>);

impl Thread {
    fn new(handle: ThreadHandle) -> Self {
        Thread(Some(handle))
    }
    
    fn join(self) {
        drop(self)
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            let _ = handle.join();
        }
    }
}
