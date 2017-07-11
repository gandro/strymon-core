//! Example keeper that keeps statistics about number of words and exposes them as key-value store.
//! It assumes to be run as one worker.
//!
//! It reads input (a text) from "word_count_input" pub/sub channel, and stores statistics in its
//! state.
//!
//! # Query format
//! This keeper returns key value pairs of the given range of keys.
//! It understands the following query format:
//!
//! `<SKP>;<EKP>`
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
//! # Response format
//! In response to a query the keeper returns zero or more tuples of format:
//!
//! `<KEY>;<VAL>`
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
use timely_keepers::keeper::{Connector, StateOperatorBuilder};

fn parse_query(query: &str) -> Option<(String, String)> {
    lazy_static! {
        static ref RE_QUERY: Regex = Regex::new("^([a-z#|]+);([a-z#|]+)$").unwrap();
    }
    let caps = RE_QUERY.captures(query);

    match caps {
        Some(caps) => {
            if caps.len() < 3 {
                return None;
            }
            let start = if let Some(start) = caps.get(1) {
                start.as_str()
            } else {
                return None;
            };
            let end = if let Some(end) = caps.get(2) {
                end.as_str()
            } else {
                return None;
            };
            if start >= end {
                return None;
            }

            Some((start.to_string(), end.to_string()))
        }
        None => None,
    }
}

fn main() {
    timely_query::execute(|root, coord| {
        let (mut input, cap) = root.dataflow::<i32, _, _>(|scope| {
            let (input_tuple, text_stream) = scope.new_unordered_input::<String>();
            let mut connector = Connector::<String, String, _, _>::new(None, scope).unwrap();
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

            let state_operator = StateOperatorBuilder::new("WordCountState",
                                                           word_count,
                                                           &word_stream,
                                                           &clients_stream,
                                                           scope.index(),
                                                           |state, update| {
                                                               let mut state = state.borrow_mut();
                                                               *state.entry(update.to_string())
                                                                    .or_insert(0) += 1;
                                                           },
                                                           |update| vec![update.to_string()],
                                                           |state| {
                let state = state.borrow();
                let mut dump = Vec::new();
                for (key, value) in state.iter() {
                    dump.push(format!("{}:{}", key, value));
                }
                dump
            })
                    .set_query_logic(|state, query| {
                        let state = state.borrow();
                        let mut resp = Vec::new();
                        match parse_query(query) {
                            Some((start, end)) => {
                                for (key, value) in state.range(start..end) {
                                    resp.push(format!("{}:{}", key, value));
                                }
                            }
                            None => (),
                        }
                        resp
                    })
                    .construct();
            let responses_stream = state_operator.get_outgoing_responses_stream();
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
