/// This file contains Messenger - helper structure for receiving and sending MessageBufs
/// over TCP connection using Abomonation serialization library.
use std::io;
use std::marker::PhantomData;

use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use futures::stream::Stream;
use futures::{Poll, Async};

use strymon_communication::message::MessageBuf;
use strymon_communication::transport::{Receiver, Sender};

/// Messenger is able to send and receive messages of given types using the given connection
/// endpoints.
/// For sending messages it exposes `send_message` function. For receiving it implements
/// Stream from futures crate.
/// 
/// # Example:
/// ```
/// # extern crate strymon_communication;
/// # extern crate strymon_keepers;
/// # extern crate futures;
/// # fn main() {
/// use futures::{Async, Stream};
/// use strymon_keepers::keeper::messenger::Messenger;
/// use strymon_communication::Network;
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
pub struct Messenger<I, O> {
    tx: Sender,
    _out_marker: PhantomData<O>,
    rx: Receiver,
    _in_marker: PhantomData<I>,
}

impl<I, O> Messenger<I, O>
    where I: DeserializeOwned, O: Serialize
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
        self.tx.send(MessageBuf::new::<O>(msg)?);
        Ok(())
    }
}

impl<I, O> Stream for Messenger<I, O>
    where I: DeserializeOwned, O: Serialize
{
    type Item = I;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(match try_ready!(self.rx.poll()) {
            Some(mut buf) => {
                let msg = buf.pop::<I>()?;
                Some(msg)
            }
            None => None,

        }))
    }
}
