// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate timely;
extern crate futures;
extern crate serde;
extern crate slab;
extern crate tokio_core;
#[macro_use]
extern crate log;
extern crate typename;
#[cfg(test)]
extern crate serde_test;
#[macro_use]
extern crate serde_derive;

extern crate strymon_communication;

pub mod publisher;
pub mod protocol;
