extern crate timely;
extern crate timely_keepers;
extern crate timely_query;

use std::{thread, time};
use std::collections::HashMap;

use timely::dataflow::operators::{Input, Inspect};
use timely::dataflow::channels::pact::Pipeline;
use timely_keepers::core::{Connector, StateOperatorBuilder};
use timely_keepers::core::data::ClientQueryResponse;


fn main() {
    timely_query::execute(|root, coord| {
        let mut input = root.dataflow::<u32, _, _>(|scope| {
            let mut connector = Connector::new(None, scope, 0).unwrap();
            coord.register_keeper("PrototypeKeeper", connector.external_addr()).unwrap();
            println!("Keeper registered");
            let clients_stream =
                connector.incoming_stream().inspect(|x| println!("From client: {:?}", x));

            let (input, data_stream) = scope.new_input::<(i32, String, i64)>();
            let data_stream = data_stream.inspect(|x| println!("Data: {:?}", x));

            let state_map = HashMap::<String, i64>::new();

            let state_operator =
                StateOperatorBuilder::new(state_map, &data_stream, &clients_stream)
                    .state_unary_stream(Pipeline, "KeyValueState", |state, input, output| {
                        input.for_each(|cap, data| {
                            let mut state = state.borrow_mut();
                            let mut session = output.session(&cap);
                            for &(action, ref key, value) in data.iter() {
                                if action > 0 {
                                    state.insert(key.clone(), value.clone());
                                } else {
                                    state.remove(key);
                                }
                                session.give((action, key.to_string(), value));
                            }
                        });
                    })
                    .clients_unary_stream(Pipeline, "KeyValueResponder", |state, input, output| {
                        input.for_each(|cap, data| {
                            let st = state.borrow();
                            let mut session = output.session(&cap);
                            for cq in data.iter() {
                                let query = cq.query();
                                let mut resp_str = String::new();
                                if let Some(value) = st.get(&query) {
                                    resp_str = format!("{}: {}", query, value);
                                }
                                let resp = ClientQueryResponse::new(&resp_str, cq);
                                session.give(resp);
                            }
                        });
                    })
                    .construct();
            let out_stream = state_operator.get_outgoing_clients_stream()
                                           .inspect(|x| println!("Respond to client: {:?}", x));
            connector.outgoing_stream(out_stream);
            input
        });

        // Let's introduce some data into system. The sleeps simulate delays from a real system.
        // It will take ~5h to introduce all the data.
        // The state we want to keep is a key-value store. Input tuples are of format:
        // (ACTION, KEY, VALUE)
        // Where ACTION is one of {+1, -1}. +1 means ADD or UPDATE, -1 means REMOVE
        // KEY is a string - the key
        // VALUE is a number - the value
        println!("let's start introducing values");
        for round in 0..10000 {
            thread::sleep(time::Duration::from_secs(2));
            input.send((1, format!("key_{}", round), 1));
            input.send((1, "key_sum".to_string(), round));
            input.advance_to((round + 1) as u32);
            root.step();
        }
    })
            .unwrap();
}
