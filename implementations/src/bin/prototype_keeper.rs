extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate implementations;

use std::collections::HashMap;
use std::time::Instant;
use std::vec::Vec;

use timely::dataflow::operators::{Input, Inspect};
use timely_keepers::keeper::{Connector, StateOperatorBuilder};

use implementations::{PrototypeQueryType, PrototypeKeyValueUpdate};

// RustFmt is very bad at formatting StateOperatorBuilder arguments
#[cfg_attr(rustfmt, rustfmt_skip)]
fn main() {
    timely_query::execute(|root, coord| {
        let mut input = root.dataflow::<u32, _, _>(|scope| {
            let mut connector =
                Connector::<PrototypeQueryType, PrototypeKeyValueUpdate<i64>, _, _>::new(None,
                                                                                         scope)
                        .unwrap();
            connector.register_with_coordinator("PrototypeKeeper", &coord).unwrap();
            println!("Keeper registered");
            let clients_stream =
                connector.incoming_stream().inspect(|x| println!("From client: {:?}", x));

            let (input, data_stream) = scope.new_input::<PrototypeKeyValueUpdate<i64>>();
            let data_stream = data_stream.inspect(|x| println!("Data: {:?}", x));

            let state_map = HashMap::<String, i64>::new();

            let state_operator = StateOperatorBuilder::new(
                "PrototypeKeeperState",
                state_map,
                &data_stream,
                &clients_stream,
                scope.index(),
                |state, update| {
                    let mut state = state.borrow_mut();
                    match update {
                        &PrototypeKeyValueUpdate::Existing { ref key, ref value } => {
                            state.insert(key.clone(), value.clone());
                        }
                        &PrototypeKeyValueUpdate::Removed { ref key } => {
                            state.remove(key);
                        }
                    }
                },
                |update| vec![update.clone()],
                |state| {
                    // Dump state logic.
                    let state = state.borrow();
                    let mut dump = Vec::new();
                    for (key, value) in state.iter() {
                        dump.push(PrototypeKeyValueUpdate::Existing {
                                      key: key.clone(),
                                      value: value.clone(),
                                  });
                    }
                    dump
                })
                    .set_query_logic(|state, query| {
                        let state = state.borrow();
                        let mut resp = Vec::new();
                        let &PrototypeQueryType::ValueFor(ref query) = query;
                        resp.push(match state.get(query) {
                                      Some(value) => {
                                          PrototypeKeyValueUpdate::Existing {
                                              key: query.clone(),
                                              value: value.clone(),
                                          }
                                      }
                                      None => {
                                          PrototypeKeyValueUpdate::Removed { key: query.clone() }
                                      }
                                  });
                        resp
                    })
                    .construct();
            let out_stream = state_operator.get_outgoing_responses_stream()
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
        if root.index() == 0 {
            println!("let's start introducing values");
            for round in 0..10000 {
                let start = Instant::now();
                while start.elapsed().as_secs() < 5 {
                    root.step();
                }
                input.send(PrototypeKeyValueUpdate::Existing {
                               key: format!("key_{}", round),
                               value: 1,
                           });
                input.send(PrototypeKeyValueUpdate::Existing {
                               key: "key_sum".to_string(),
                               value: round,
                           });
                input.advance_to((round + 1) as u32);
                root.step();
            }
        }
    })
            .unwrap();
}
