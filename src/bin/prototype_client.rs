extern crate futures;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate timely_system;

use futures::Future;

use timely_system::network::Network;
use timely_keepers::core::Query;

fn main() {
    timely_query::execute(|_, coord| {
        let network = Network::init().unwrap();
        let keeper = coord.lookup_keeper("PrototypeKeeper").unwrap();
        println!("Keeper found: {:?}", keeper);
        let (tx, _) = network.client((&keeper.addr.0[..], keeper.addr.1)).unwrap();
        tx.request(&Query::new("key_sum"))
            .and_then(|resp| {
                          println!("Got response! '{}'", resp.text);
                          Ok(())
                      })
            .wait()
            .unwrap();
    })
            .unwrap();
}
