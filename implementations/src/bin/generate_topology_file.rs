extern crate clap;
extern crate model as topology_model;

use std::io::{BufWriter, Write};
use std::fs::File;

use clap::App;
use topology_model::topology::{Topology};
use topology_model::generate::fat_tree_topology;

fn topo_to_file(topo: &Topology, file_path: &str) {
    let mut file = BufWriter::new(File::create(file_path).unwrap());

    for s in topo.all_switches() {
        file.write(format!("AS;{}\n", s.name).as_bytes()).unwrap();
    }

    for h in topo.all_hosts() {
        let ref switch_name = topo.get_switch(h.switch_id).name;
        file.write(format!("AH;{};{}\n", h.name, switch_name).as_bytes()).unwrap();
    }

    for c in topo.all_connections() {
        let ref sn1 = topo.get_switch(c.from).name;
        let ref sn2 = topo.get_switch(c.from).name;
        file.write(format!("AC;{};{};{}\n", sn1, sn2, c.weight).as_bytes()).unwrap();
    }
}


fn main() {
    let cmd_args = App::new("generate_topology_file")
        .about("Generate file containing topology in form that can be ingested by topology keeper.")
        .args_from_usage("<OUTPUT> 'Output file.'")
        .get_matches();
    let file_path = cmd_args.value_of("OUTPUT").unwrap().to_string();

    let topology = fat_tree_topology(20, 10, false);
    topo_to_file(&topology, &file_path);
}
