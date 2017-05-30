extern crate clap;
extern crate timely;
extern crate timely_query;

use std::io::{BufRead, BufReader};
use std::fs::File;

use clap::App;
use timely::dataflow::operators::Input;
use timely_query::publish::Partition as Part;

fn main() {
    let cmd_args = App::new("word_count_input_producer")
        .about("Generate input for Word Count Keeper")
        .args_from_usage("<INPUT> 'The file containing the text to publish'")
        .get_matches();

    let text_file_path = cmd_args.value_of("INPUT").unwrap().to_string();

    timely_query::execute(move |root, coord| {
        let mut input = root.dataflow::<i32, _, _>(|scope| {
            let (input, stream) = scope.new_input();
            coord.publish("word_count_input", &stream, Part::Merge).unwrap();
            input
        });

        let text_file = BufReader::new(File::open(&text_file_path).unwrap());
        for line in text_file.lines() {
            input.send(line.unwrap().to_string());
            root.step();
        }

    })
            .unwrap();
}
