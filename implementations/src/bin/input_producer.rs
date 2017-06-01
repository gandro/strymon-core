//! It publishes lines of given file as tuples on the specified pubish subscribe channel.
extern crate clap;
extern crate timely;
extern crate timely_query;

use std::io::{BufRead, BufReader};
use std::fs::File;

use clap::App;
use timely::dataflow::operators::Input;
use timely_query::publish::Partition as Part;

fn main() {
    let cmd_args = App::new("input_producer")
        .about("Generate input for any dataflow subscribed to the given channel.")
        .args_from_usage("<CHANNEL> 'The channel on which to publish the lines.'")
        .args_from_usage("<INPUT> 'The file containing the lines to publish.'")
        .get_matches();

    let channel = cmd_args.value_of("CHANNEL").unwrap().to_string();
    let file_path = cmd_args.value_of("INPUT").unwrap().to_string();

    timely_query::execute(move |root, coord| {
        let mut input = root.dataflow::<i32, _, _>(|scope| {
            let (input, stream) = scope.new_input();
            coord.publish(&channel, &stream, Part::Merge).unwrap();
            input
        });

        let file = BufReader::new(File::open(&file_path).unwrap());
        for line in file.lines() {
            input.send(line.unwrap().trim().to_string());
            root.step();
        }

    })
            .unwrap();
}
