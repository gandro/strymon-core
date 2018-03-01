// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate serde;
extern crate serde_json;
extern crate futures;

extern crate strymon_executor;
extern crate strymon_coordinator;
extern crate strymon_communication;
extern crate strymon_model;
extern crate strymon_rpc;

mod status;
mod submit;
mod terminate;
mod manage;

use std::env;

use clap::{App, AppSettings, Arg};
use env_logger::{Builder, Target};
use failure::Error;

fn dispatch() -> Result<(), Error> {
    let matches = App::new("Strymon")
        .version("0.1")
        .author("Systems Group, ETH Zürich")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(status::usage())
        .subcommand(submit::usage())
        .subcommand(terminate::usage())
        .subcommand(manage::usage())
        .arg(
            Arg::with_name("log-level")
                .short("l")
                .long("log-level")
                .takes_value(true)
                .value_name("RUST_LOG")
                .help("Set level and filters for logging"),
        )
        .get_matches();

    // configure env_logger
    let mut logger = Builder::new();
    logger.target(Target::Stderr);
    if let Some(s) = matches.value_of("log-level") {
        logger.parse(s);
    } else if let Ok(s) = env::var("RUST_LOG") {
        logger.parse(&s);
    }
    logger.init();

    match matches.subcommand() {
        ("status", Some(args)) => status::main(args),
        ("submit", Some(args)) => submit::main(args),
        ("terminate", Some(args)) => terminate::main(args),
        ("manage", Some(args)) => manage::main(args),
        _ => unreachable!("invalid subcommand"),
    }
}

fn main() {
    if let Err(err) = dispatch() {
        eprintln!("Error: {}", err);

        for cause in err.causes().skip(1) {
            eprintln!("Caused by: {}", cause);
        }

        for line in err.backtrace().to_string().lines() {
            error!("{}", line);
        }

        std::process::exit(1);
    }
}
