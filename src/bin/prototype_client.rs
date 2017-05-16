extern crate futures;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate timely_system;

use timely::dataflow::operators::{Input, Inspect};

use timely_keepers::client::KeeperQuery;

fn main() {
    timely_query::execute(|root, coord| {
        let mut input = root.dataflow::<u32, _, _>(|scope| {
            let (input, stream) = scope.new_input::<String>();
            stream.inspect(|x| println!("Seeing: {:?}", x));
            input
        });

        let keeper_data = KeeperQuery::new("key_sum", "PrototypeKeeper", &coord).unwrap();
        let mut round = 0;
        for data in keeper_data {
            round += 1;
            input.send(data);
            input.advance_to(round);
            root.step();
        }
        println!("Stream from Keeper finished");
    })
            .unwrap();
}
