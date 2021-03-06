// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::ops::RangeFrom;
use std::marker::PhantomData;

pub struct Generator<T> {
    generator: RangeFrom<u64>,
    marker: PhantomData<T>,
}

impl<T: From<u64>> Generator<T> {
    pub fn new() -> Self {
        Generator {
            generator: 0..,
            marker: PhantomData,
        }
    }

    pub fn generate(&mut self) -> T {
        From::from(self.generator.next().unwrap())
    }
}
