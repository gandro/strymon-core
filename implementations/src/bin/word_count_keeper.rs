//! [WIP!]
//! Example keeper that keeps statistics about number of words and exposes them as key-value store.
//!
//! It reads input (a text) from "word_count_input" pub/sub channel, and stores statistics in its
//! state.
//!
//! # Query format
//! This keeper returns key value pairs of the given range of keys.
//! It understands the following query format:
//!
//! `<A>;<SKP>;<EKP>`
//!
//! `<SKP>` and `<EKP>` stand for, respectively, starting key prefix and ending key prefix. They are
//! to be used for expressing the range of keys that this query asks for: `[<SKP>, <EKP>)`. The
//! keys consist only of small letters of English alphabet and only those can be used in the
//! queries. The `#` and `|` characters have a special meaning in the queries: they stand for a
//! string that is lexicographically respectively lower and greater than any other string in the
//! language. For example:
//! * `a;b` - all strings that start with letter 'a'
//! * `a;|` - all possible strings
//! * `z;|` - all strings starting with letter 'z'
//! * `dog;dog#` - only 'dog' string
//! * `abba;abba|` - all strings starting with 'abba'
//!
//! `<A>` is the action to perform. It can be either `G` for "get" or `U` for "updates". When `G` is
//! used, the query gets immediate response basing on the current state and connection is closed.
//! With `U` client subscribes to any upcoming updates to the range of keys it asked for.
//!
//! # Response format
//! In response to a query the keeper returns zero or more tuples of format:
//!
//! `<CNT>;<KEY>;<VAL>`
//!
//! `<CMT>` is either `1` or `-1` (kinda like in Differential) to signalize that the key-value pair
//! was, respectively, added or removed from the store.
//!
//! `<KEY>` and `<VAL>` are the values of the key and value returned from the store.
//!
//! # Input
//! It reads input from "word_count_input" channel. The input can be any kind of text tuples - the
//! Keeper will lowercase it and remove any characters that are not from English alphabet.
#[macro_use]
extern crate lazy_static;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate regex;

use std::collections::BTreeMap;

use regex::Regex;
use timely::dataflow::operators::{Map, UnorderedInput, Inspect};
use timely::dataflow::channels::message::Content;
use timely::dataflow::channels::pact::Pipeline;
use timely_keepers::core::{Connector, StateOperatorBuilder};

enum QueryType {
    Get,
    Update,
}

fn parse_query(query: &str) -> Option<(QueryType, String, String)> {
    lazy_static! {
        static ref RE_QUERY: Regex = Regex::new("^(G|U);([a-z#|]+);([a-z#|]+)$").unwrap();
    }
    let caps = RE_QUERY.captures(query);

    match caps {
        Some(caps) => {
            if caps.len() < 4 {
                return None;
            }
            let qt = if let Some(action) = caps.get(1) {
                if action.as_str() == "G" {
                    QueryType::Get
                } else {
                    QueryType::Update
                }
            } else {
                return None;
            };
            let start = if let Some(start) = caps.get(2) {
                start.as_str()
            } else {
                return None;
            };
            let end = if let Some(end) = caps.get(3) {
                end.as_str()
            } else {
                return None;
            };
            if start >= end {
                return None;
            }

            Some((qt, start.to_string(), end.to_string()))
        }
        None => None,
    }
}

fn main() {
    timely_query::execute(|root, coord| {
        let (mut input, cap) = root.dataflow::<i32, _, _>(|scope| {
            let (input_tuple, text_stream) = scope.new_unordered_input::<String>();
            let mut connector = Connector::new(None, scope).unwrap();
            connector.register_with_coordinator("WordCountKeeper", &coord).unwrap();
            let clients_stream =
                connector.incoming_stream().inspect(|x| println!("From client: {:?}", x));

            let word_count = BTreeMap::<String, i64>::new();

            let re_no_alph = Regex::new(r"[^a-z\s]").unwrap();
            let re_space = Regex::new(r"\s+").unwrap();
//            let text_stream = text_stream.inspect(|x| println!("Data: {:?}", x));
            let word_stream =
                text_stream.map(move |x| {
                                    re_no_alph.replace_all(x.to_lowercase().trim(), "").to_string()
                                })
                    .flat_map(move |x| {
                                  re_space.split(&x).map(|s| s.to_string()).collect::<Vec<String>>()
                              });

            let state_operator =
                StateOperatorBuilder::new(word_count, &word_stream, &clients_stream)
                    .state_unary_stream(Pipeline, "WordCount", |state, input, output| {
                        input.for_each(|cap, data| {
                            let mut state = state.borrow_mut();
                            for word in data.iter() {
                                *state.entry(word.to_string()).or_insert(0) += 1;
                                output.session(&cap).give(word.to_string());
                            }
                        });
                    })
                    .clients_unary_stream(Pipeline, "WordCountResponder", |state, input, output| {
                        input.for_each(|cap, data| {
                            let state = state.borrow();
                            let mut session = output.session(&cap);
                            for cq in data.iter() {
                                let mut response = cq.create_response();
                                match parse_query(&cq.query()) {
                                    Some((action, start, end)) => {
                                        match action {
                                            QueryType::Get => {
                                                for (key, value) in state.range(start..end) {
                                                    response.add_tuple(&format!("{}:{}",
                                                                                key,
                                                                                value));
                                                }
                                            }
                                            QueryType::Update => (),
                                        }
                                    }
                                    // TODO: Insert something here that will end the connection!
                                    None => (),
                                }

                                session.give(response);
                            }
                        });
                    })
                    .construct();
            let responses_stream = state_operator.get_outgoing_clients_stream();
            connector.outgoing_stream(responses_stream);
            input_tuple
        });
        let sub = coord.subscribe::<_, String>("word_count_input".into(), cap).unwrap();
        println!("Keeper subscribed to word_count_input");
        for (time, data) in sub {
            input.session(time).give_content(&mut Content::Typed(data));
            root.step();
        }
    })
            .unwrap();
}
