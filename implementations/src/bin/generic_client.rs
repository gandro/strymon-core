extern crate clap;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;

use clap::App;
use timely::dataflow::operators::{Input, Inspect};
use timely_keepers::client::KeeperStreamBuilder;

fn main() {
    let cmd_args = App::new("generic_client")
        .args_from_usage("<KEEPER> 'The query'
                          <QUERY> 'The keeper'")
        .get_matches();

    let keeper = cmd_args.value_of("KEEPER").unwrap().trim().to_string();
    let query = cmd_args.value_of("QUERY").unwrap().trim().to_string();

    timely_query::execute(move |root, coord| {
        let mut input =
            root.dataflow::<u32, _, _>(move |scope| {
                                           let (input, stream) = scope.new_input::<String>();
                                           stream.inspect(|x| println!("Got: {}", x));
                                           input
                                       });

        let query = query.clone();
        let keeper_data = KeeperStreamBuilder::<String, String>::new(&keeper, &coord)
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
