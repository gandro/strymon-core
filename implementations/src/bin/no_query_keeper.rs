extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate implementations;

use std::time::Instant;
use std::vec::Vec;

use timely::dataflow::operators::{Input, Inspect};
use timely_keepers::keeper::{Connector, StateOperatorBuilder};
use timely_keepers::model::EmptyQT;

fn main() {
    timely_query::execute(|root, coord| {
        let mut input = root.dataflow::<u32, _, _>(|scope| {
            let mut connector = Connector::<EmptyQT, u64, _, _>::new(None, scope).unwrap();
            connector.register_with_coordinator("NoQueryKeeper", &coord).unwrap();
            println!("Keeper registered");
            let clients_stream =
                connector.incoming_stream().inspect(|x| println!("From client: {:?}", x));

            let (input, data_stream) = scope.new_input::<u64>();
            let data_stream = data_stream.inspect(|x| println!("Data: {:?}", x));

            let state = Vec::<u64>::new();

            let state_operator = StateOperatorBuilder::new("NoQueryKeeperState",
                                                           state,
                                                           &data_stream,
                                                           &clients_stream,
                                                           scope.index(),
                                                           |state, update| {
                                                               let mut state = state.borrow_mut();
                                                               state.push(update.clone());
                                                           },
                                                           |update| vec![update.clone()],
                                                           |state| {
                                                               // Dump state logic.
                                                               let state = state.borrow();
                                                               state.to_vec()
                                                           })
                    .construct();
            let out_stream = state_operator.get_outgoing_responses_stream();
            connector.outgoing_stream(out_stream);
            input
        });

        if root.index() == 0 {
            for round in 0..10000 {
                let start = Instant::now();
                while start.elapsed().as_secs() < 2 {
                    root.step();
                }
                input.send(round as u64);
                input.advance_to((round + 1) as u32);
                root.step();
            }
        }
    })
            .unwrap();
}
