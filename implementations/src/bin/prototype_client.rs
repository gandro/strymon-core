extern crate futures;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate implementations;

use timely::dataflow::operators::{Input, Inspect};

use timely_keepers::client::KeeperConnection;

use implementations::{PrototypeQueryType, PrototypeKeyValueUpdate};

fn main() {
    timely_query::execute(|root, coord| {
        let mut input = root.dataflow::<u32, _, _>(|scope| {
            let (input, stream) = scope.new_input::<PrototypeKeyValueUpdate<i64>>();
            stream.inspect(|x| println!("Seeing: {:?}", x));
            input
        });

        let query = PrototypeQueryType::ValueFor("key_sum".to_string());
        let keeper_data =
            KeeperConnection::<PrototypeQueryType, PrototypeKeyValueUpdate<i64>>::new(
                "PrototypeKeeper",
                &coord)
                    .query(query)
                    .unwrap();
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
