extern crate clap;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;

use clap::App;
use timely::dataflow::operators::{Input, Inspect};
use timely_keepers::client::KeeperStreamBuilder;

fn main() {
    let cmd_args = App::new("word_count_client")
        .args_from_usage("<RANGE_START> 'Range start'
                          <RANGE_END> 'Range end'")
        .get_matches();
    let range_start = cmd_args.value_of("RANGE_START")
        .unwrap()
        .to_string()
        .to_lowercase();
    let range_end = cmd_args.value_of("RANGE_END")
        .unwrap()
        .to_string()
        .to_lowercase();
    timely_query::execute(move |root, coord| {
        let mut input =
            root.dataflow::<u32, _, _>(move |scope| {
                                           let (input, stream) = scope.new_input::<String>();
                                           stream.inspect(|x| println!("Got: {}", x));
                                           input
                                       });

        let query = format!("{};{}", range_start, range_end);
        let keeper_data = KeeperStreamBuilder::<String, String>::new("WordCountKeeper", &coord)
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
