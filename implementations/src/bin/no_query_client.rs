extern crate futures;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate implementations;

use timely::dataflow::operators::{Input, Inspect};
use timely_keepers::model::StateRequest;
use timely_keepers::client::KeeperConnection;
use timely_keepers::model::EmptyQT;

fn main() {
    timely_query::execute(|root, coord| {
        let mut input = root.dataflow::<u32, _, _>(|scope| {
            let (input, stream) = scope.new_input::<u64>();
            stream.inspect(|x| println!("Seeing: {:?}", x));
            input
        });

        let keeper_stream = KeeperConnection::<EmptyQT, u64>::new("NoQueryKeeper", &coord)
            .state(StateRequest::StateAndUpdates)
            .unwrap();
        let mut round = 0;
        for data in keeper_stream {
            input.send(data);
            round += 1;
            input.advance_to(round);
            root.step();
        }
        println!("We should never get here since stream from the Keeper never finishes!");
    })
            .unwrap();
}
