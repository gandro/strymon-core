// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The publisher logic and the interfaces used to control it.

use std::io;
use std::thread;

use slab::Slab;
use serde::Serialize;

use timely::ExchangeData;
use timely::progress::timestamp::Timestamp;
use timely::dataflow::operators::capture::event::Event as TimelyEvent;
use tokio_core::reactor::{Core, Handle};

use strymon_communication::Network;
use strymon_communication::transport::{Listener, Sender, Receiver};
use strymon_communication::message::MessageBuf;

use futures::future::Future;
use futures::stream::{self, Stream};
use futures::unsync::mpsc;

use protocol::{Message, RemoteTimestamp};

use self::progress::{LowerFrontier, UpperFrontier};
use self::sink::{EventSink, EventStream};

pub mod sink;
pub mod progress;

type SubscriberId = usize;

enum Event<T, D> {
    Timely(TimelyEvent<T, D>),
    Accepted((Sender, Receiver)),
    Disconnected(SubscriberId),
    Error(SubscriberId, io::Error),
    ShutdownRequested,
}

/// State and logic of the publisher.
///
/// Maintains the upper and lower frontier of a Timely stream and broadcasts
/// their updated versions and any incoming data tuples to subscribed clients.
struct PublisherServer<T: Timestamp, D> {
    // progress tracking state
    lower: LowerFrontier<T>,
    upper: UpperFrontier<T>,
    // connected subscribers
    subscribers: Slab<Sender>,
    // tokio event loop
    events: Box<Stream<Item=Event<T, D>, Error=io::Error>>,
    notificator: mpsc::UnboundedSender<Event<T, D>>,
    core: Core,
    handle: Handle,
}

impl<T: RemoteTimestamp, D: ExchangeData + Serialize> PublisherServer<T, D> {
    /// Creates a new publisher, accepting subscribers on `socket`, publishing
    /// the Timely events observed on `stream`.
    fn new(socket: Listener, stream: EventStream<T, D>) -> io::Result<Self> {
        let core = Core::new()?;
        let handle = core.handle();

        // queue for disconnection events from subscribers
        let (notificator, subscribers) = mpsc::unbounded();

        // we have three event sources:
        let listener = socket.map(Event::Accepted);
        let timely = stream.map(Event::Timely).map_err(|_| unreachable!())
            .chain(stream::once(Ok(Event::ShutdownRequested)));
        let subscribers = subscribers.map_err(|_| unreachable!());
        // all of which we merge into a single stream
        let events = listener.select(subscribers).select(timely);

        Ok(PublisherServer {
            lower: LowerFrontier::default(),
            upper: UpperFrontier::empty(),
            subscribers: Slab::new(),
            events: Box::new(events),
            notificator: notificator,
            core: core,
            handle: handle,
        })
    }

    /// Starts serving subscribers, blocks until the Timely stream completes
    /// (or an error happens).
    fn serve(mut self) -> io::Result<()> {
        loop {
            // run tokio reactor until we get the next event
            let event = {
                let events = self.events.into_future();
                match self.core.run(events) {
                    Ok((ev, events)) => {
                        // restore ownership
                        self.events = events;
                        ev.unwrap()
                    },
                    Err((err, _)) => return Err(err),
                }
            };

            match event {
                // processing incoming timely events
                Event::Timely(ev) => self.timely_event(ev)?,
                // handle networking events
                Event::Accepted(sub) => self.add_subscriber(sub),
                Event::Disconnected(id) => self.remove_subscriber(id),
                Event::Error(id, err) => {
                    // subscriber errors should not be fatal. we just log
                    // them and forget about it.
                    error!("Subscriber {}: {}", id, err);
                }
                Event::ShutdownRequested => {
                    // this drains the queues of all still connected subscribers
                    drop(self.subscribers);
                    return Ok(());
                }
            }
        }
    }

    /// Sends `msg` to all connected subscribers.
    fn broadcast(&self, msg: MessageBuf) {
        let last = self.subscribers.len();
        for (i, sub) in self.subscribers.iter().map(|(id, s)| (id+1, s)) {
            if i < last {
                sub.send(msg.clone());
            } else {
                // this case is a hint to the compiler that for the last
                // iteration we can move `msg` directly, no need to clone
                sub.send(msg);
                break;
            }
        }
    }

    /// Processes a single Timely event, might cause multiple messages to be
    /// sent to connected subscribers.
    fn timely_event(&mut self, event: TimelyEvent<T, D>) -> io::Result<()> {
        match event {
            TimelyEvent::Progress(updates) => {
                let frontier_changed = self.lower.update(updates);
                if frontier_changed {
                    let lower = Message::<T, D>::lower(self.lower.elements());
                    self.broadcast(MessageBuf::new(lower)?);
                }
            },
            TimelyEvent::Messages(time, data) => {
                let new_epoch = !self.upper.greater_than(&time);
                if new_epoch {
                    {
                        let upper = Message::<T, D>::upper(self.upper.elements());
                        self.broadcast(MessageBuf::new(upper)?);
                    }
                    let updated = self.upper.insert(time.clone());
                    debug_assert!(updated, "new epoch did not change upper?!");
                }
                let data = Message::<T, D>::data(time, data);
                self.broadcast(MessageBuf::new(data)?);
            }
        };

        Ok(())
    }

    /// Registers a new subscriber.
    ///
    /// Installs a "monitor" for the subscriber, making sure we get notified
    /// when it disconnects.
    fn add_subscriber(&mut self, (tx, rx): (Sender, Receiver)) {
        let id = self.subscribers.insert(tx);
        let notificator = self.notificator.clone();

        let subscriber = rx
            .for_each(|_| {
                Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected message"))
            })
            .then(move |res| {
                let event = match res {
                    Ok(()) => Event::Disconnected(id),
                    Err(err) => Event::Error(id, err),
                };

                notificator.unbounded_send(event).map_err(|_| ())
            });

        self.handle.spawn(subscriber);
    }

    /// Removes a subscriber from the broadcasting list.
    ///
    /// This does not cancel the subscriber monitor registered above, so if the
    /// subscriber is still alive, it will still emit events on errors or
    /// when it disconnects.
    fn remove_subscriber(&mut self, id: SubscriberId) {
        self.subscribers.remove(id);
    }
}

/// The host and port on which the publisher is accepting subscribers.
pub type Addr = (String, u16);

/// A handle for spawned publisher.
///
/// Note the the drop order is important here: The event `EventSink` must be
/// dropped before `Thread` in order to avoid a deadlock: Dropping `EventSink`
/// indicates to the publisher thread that it has to shut down, which will block
/// the join operation until the shutdown is complete.
pub struct Publisher<T, D> {
    /// Handle for events to be published by this instance.
    pub sink: EventSink<T, D>,
    /// Address of the socket on which this publisher is accepting subscribers.
    pub addr: Addr,
    /// A join handle for the spawned thread.
    pub thread: Thread,
}

impl<T, D> Publisher<T, D> where T: RemoteTimestamp, D: ExchangeData + Serialize {
    /// Spawns a new publisher thread on a ephemerial network port.
    ///
    /// The corresponding address can be obtained through `Publisher::addr` and
    /// needs to be registered in the catalog seperately.
    pub fn new(network: &Network) -> io::Result<Self> {
        // the queue between the Timely operator and this publisher thread
        let (timely_sink, timely_stream) = sink::pair();

        // the network socket on which subscribers are accepted
        let listener = network.listen(None)?;
        let addr = {
            let (host, port) = listener.external_addr();
            (String::from(host), port)
        };

        // main event loop of the publisher thread
        let handle = thread::spawn(move || {
            PublisherServer::new(listener, timely_stream)
                .and_then(|publisher| publisher.serve())
        });

        Ok(Publisher {
            sink: timely_sink,
            addr: addr,
            thread: Thread::new(handle),
        })
    }
}

type ThreadHandle = thread::JoinHandle<io::Result<()>>;

/// A join handle for the publisher thread.
///
/// This blocks when dropped (or when calling the `join` method), to ensure all
/// subscriber queues are drained properly.
///
/// **Note**: Dropping this handle before dropping the corresponding `EventSink`
/// will cause a deadlock.
pub struct Thread(Option<ThreadHandle>);

impl Thread {
    fn new(handle: ThreadHandle) -> Self {
        Thread(Some(handle))
    }

    fn join_mut(&mut self) -> io::Result<()> {
        match self.0.take().map(|t| t.join()) {
            Some(Ok(res)) => res,
            Some(Err(_)) => Err(io::Error::new(io::ErrorKind::Other, "thread panicked")),
            None => Err(io::Error::new(io::ErrorKind::Other, "already joined")),
        }
    }

    /// Waits for the publisher to drain its queues.
    pub fn join(mut self) -> io::Result<()> {
        self.join_mut()
    }

    /// Drops this handle without waiting for the publisher thread to finish.
    pub fn forget(mut self) {
        self.0.take();
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        let _ = self.join_mut();
    }
}
