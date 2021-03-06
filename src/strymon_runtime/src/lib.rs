// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate libc;
#[macro_use]
extern crate log;
#[macro_use]
extern crate futures;
extern crate tokio_io;
extern crate tokio_core;
extern crate tokio_signal;
extern crate tokio_process;

extern crate timely;
extern crate timely_communication;

extern crate rand;
extern crate time;

extern crate serde;
extern crate typename;

extern crate strymon_communication;

extern crate strymon_model;

extern crate strymon_rpc;

pub mod coordinator;
pub mod executor;
#[deprecated(since = "0.1.2", note="The strymon_runtime::query module has been replaced by the strymon_job crate.")]
pub mod query;
mod pubsub;
pub mod submit;
