/// This file contains Messenger - helper structure for receiving and sending MessageBufs
/// over TCP connection using Abomonation serialization library.
use std::io;
use std::any::Any;
use std::marker::PhantomData;

use abomonation::Abomonation;
use futures::stream::Stream;
use futures::{Poll, Async};

use timely_system::network::message::abomonate::{Abomonate, NonStatic};
use timely_system::network::message::MessageBuf;
use timely_system::network::{Receiver, Sender};

/// Messenger is able to send and receive messages of given types using the given connection
/// endpoints.
/// For sending messages it exposes `send_message` function. For receiving it implements
/// Stream from futures crate.
/// 
/// # Example:
/// ```
/// # extern crate timely_system;
/// # extern crate timely_keepers;
/// # extern crate futures;
/// # fn main() {
/// use futures::{Async, Stream};
/// use timely_keepers::keeper::messenger::Messenger;
/// use timely_system::network::Network;
/// 
/// let network = Network::init().unwrap();
/// let listener = network.listen(None).unwrap();
/// let (client_tx, client_rx) = network.connect(listener.external_addr()).unwrap();
/// let listener = listener.fuse();
/// let (server_tx, server_rx) = listener.wait().next().unwrap().unwrap();
/// let server_messenger = Messenger::<String, String>::new(server_tx, server_rx);
/// let client_messenger = Messenger::<String, String>::new(client_tx, client_rx);
/// client_messenger.send_message("Hello!".to_string()).unwrap();
/// let msg = server_messenger.wait().next().unwrap().unwrap();
/// assert_eq!(msg, "Hello!");
/// # }
/// ```
pub struct Messenger<I, O>
    where I: Abomonation + Any + Clone + NonStatic,
          O: Abomonation + Any + Clone + NonStatic
{
    tx: Sender,
    _out_marker: PhantomData<O>,
    rx: Receiver,
    _in_marker: PhantomData<I>,
}

impl<I, O> Messenger<I, O>
    where I: Abomonation + Any + Clone + NonStatic,
          O: Abomonation + Any + Clone + NonStatic
{
    pub fn new(tx: Sender, rx: Receiver) -> Self {
        Messenger {
            tx: tx,
            rx: rx,
            _out_marker: PhantomData,
            _in_marker: PhantomData,
        }
    }

    pub fn send_message(&self, msg: O) -> io::Result<()> {
        let mut buf = MessageBuf::empty();
        buf.push::<Abomonate, O>(&msg).expect("Encoding error shouldn't happen with Abomonate.");
        self.tx.send(buf);
        Ok(())
    }
}

impl<I, O> Stream for Messenger<I, O>
    where I: Abomonation + Any + Clone + NonStatic,
          O: Abomonation + Any + Clone + NonStatic
{
    type Item = I;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(match try_ready!(self.rx.poll()) {
            Some(mut buf) => {
                let msg = buf.pop::<Abomonate, I>().map_err(Into::<io::Error>::into)?;
                Some(msg)
            }
            None => None,

        }))
    }
}
