use std::io;

use futures::Poll;
use futures::sync::mpsc;
use futures::stream::Stream;
use futures::future::{Future, Executor};

use tokio_core::reactor::Handle;

use strymon_communication::Network;
use strymon_communication::transport::{Listener, Sender, Receiver};

/// Handle for a running pub-sub server.
pub struct PubSubServer {
    rx: mpsc::UnboundedReceiver<SubscriberEvent>,
}

impl PubSubServer {
    /// Creates a new pub-sub server on the given socket.
    ///
    /// Consumes the given Listener and runs a pub-sub logic on it. 
    pub fn from_listener(listener: Listener, executor: &Handle) -> Self {
        // queue of subscriber events
        let (tx, rx) = mpsc::unbounded();

        // The pubsub server logic is as follows:
        //  1. When a new subscriber arrives, we emit a `SubscriberAccepted`
        //     event, handing over the outgoing queue handle to the main
        //     event loop.
        //  2. We listen on incoming queue handle to be notified when the
        //     subscriber drops, so the main event loop can deregister it.
        //     We do not expect the subscriber to ever send us any message.
        let handle = executor.clone();
        let server = listener.for_each(move |(tx, rx)| {
            
            
            Ok(())
        }).map_err(|err| error!("PubSub server error: {}", err));

        // the logic runs on the specified event loop
        executor.spawn(server);

        PubSubServer { rx }
    }
}

pub type SubscriberId = usize;

pub enum SubscriberEvent {
    Accepted(SubscriberId, Sender),
    Error(SubscriberId, io::Error),
    Disconnected(SubscriberId),
}

impl Stream for PubSubServer {
    type Item = SubscriberEvent;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll()
    }
}
