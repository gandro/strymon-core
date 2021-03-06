// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io::Error;

use futures::future::Future;
use tokio_core::reactor::Handle;

use strymon_communication::rpc::{Outgoing, Request, RequestBuf};

use super::handler::CoordinatorRef;
use strymon_rpc::coordinator::*;

pub struct Dispatch {
    coord: CoordinatorRef,
    handle: Handle,
    tx: Outgoing,
}

impl Dispatch {
    pub fn new(coord: CoordinatorRef, handle: Handle, tx: Outgoing) -> Self {
        debug!("dispatching on new incoming connection");
        Dispatch {
            coord: coord,
            handle: handle,
            tx: tx,
        }
    }

    pub fn dispatch<'a>(&'a mut self, req: RequestBuf<CoordinatorRPC>) -> Result<(), Error> {
        debug!("dispatching request {:?}", req.name());
        match *req.name() {
            Submission::NAME => {
                let (req, resp) = req.decode::<Submission>()?;
                let submission = self.coord
                    .submission(req)
                    .then(|res| Ok(resp.respond(res)));

                self.handle.spawn(submission);
            }
            Termination::NAME => {
                let (req, resp) = req.decode::<Termination>()?;
                let termination = self.coord
                    .termination(req)
                    .then(|res| Ok(resp.respond(res)));

                self.handle.spawn(termination);
            }
            AddWorkerGroup::NAME => {
                let (AddWorkerGroup { query, group }, resp) =
                    req.decode::<AddWorkerGroup>()?;
                let response = self.coord
                    .add_worker_group(query, group)
                    .then(|res| Ok(resp.respond(res)));
                self.handle.spawn(response);
            }
            AddExecutor::NAME => {
                let (req, resp) = req.decode::<AddExecutor>()?;
                let id = self.coord.add_executor(req, self.tx.clone());
                resp.respond(Ok((id)));
            }
            Publish::NAME => {
                let (req, resp) = req.decode::<Publish>()?;
                resp.respond(self.coord.publish(req));
            }
            Unpublish::NAME => {
                let (Unpublish { token, topic }, resp) = req.decode::<Unpublish>()?;
                resp.respond(self.coord.unpublish(token, topic));
            }
            Subscribe::NAME => {
                let (req, resp) = req.decode::<Subscribe>()?;
                let subscribe = self.coord
                    .subscribe(req)
                    .then(|res| Ok(resp.respond(res)));
                self.handle.spawn(subscribe);
            }
            Unsubscribe::NAME => {
                let (Unsubscribe { token, topic }, resp) = req.decode::<Unsubscribe>()?;
                resp.respond(self.coord.unsubscribe(token, topic));
            }
            Lookup::NAME => {
                let (Lookup { name }, resp) = req.decode::<Lookup>()?;
                resp.respond(self.coord.lookup(&name));
            }
            AddKeeperWorker::NAME => {
                let (AddKeeperWorker { name, worker_num, addr }, resp) =
                    req.decode::<AddKeeperWorker>()?;
                resp.respond(self.coord.add_keeper_worker(name, worker_num, addr));
            }
            GetKeeperAddress::NAME => {
                let (GetKeeperAddress { name }, resp) = req.decode::<GetKeeperAddress>()?;
                resp.respond(self.coord.get_keeper_address(name));
            }
            RemoveKeeperWorker::NAME => {
                let (RemoveKeeperWorker { name, worker_num }, resp) =
                    req.decode::<RemoveKeeperWorker>()?;
                resp.respond(self.coord.remove_keeper_worker(name, worker_num));
            }
        }

        Ok(())
    }
}
