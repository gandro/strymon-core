// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::{TcpListener, TcpStream};
use std::io::{copy, Result, Error, ErrorKind};
use std::fs::{self, File};
use std::path::Path;
use std::thread;

use Network;

pub struct Handle {
    url: String,
    listener: TcpListener,
}

impl Handle {
    pub fn url(&self) -> String {
        self.url.clone()
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        // TODO(swicki) this is a hack, but we need to interrupt the listener
        drop(self.listener.set_nonblocking(true))
    }
}

#[cfg(unix)]
fn fix_permissions<P: AsRef<Path>>(path: P) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perm = fs::metadata(&path)?.permissions();
    perm.set_mode(0o700);

    fs::set_permissions(&path, perm)
}

#[cfg(windows)]
fn fix_permissions<P: AsRef<Path>>(_: P) -> Result<()> { Ok(()) }

impl Network {
    pub fn upload<P: AsRef<Path>>(&self, path: P) -> Result<Handle> {
        let path = path.as_ref().to_owned();
        if !path.is_file() {
            return Err(Error::new(ErrorKind::NotFound, "file not found"));
        }

        let listener = TcpListener::bind("0.0.0.0:0")?;
        let port = listener.local_addr()?.port();
        let interrupt = listener.try_clone()?;

        thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                let mut file = File::open(&path).unwrap();
                thread::spawn(move || {
                    if let Err(err) = copy(&mut file, &mut stream) {
                        error!("while uploading file: {}", err);
                    }
                });
            }
        });

        Ok(Handle {
            url: format!("tcp://{}:{}", self.hostname, port),
            listener: interrupt,
        })
    }

    pub fn download<P: AsRef<Path>>(&self, url: &str, path: P) -> Result<()> {
        // url should have the format: "tcp://host:port"
        if !url.starts_with("tcp://") {
            return Err(Error::new(ErrorKind::InvalidInput,
                                  "url doesn't start with tcp://'"));
        }

        let addr = &url[6..];
        let mut stream = TcpStream::connect(addr)?;
        let mut file = File::create(&path)?;
        fix_permissions(&path)?;

        debug!("downloading file from tcp://{} to {:?}", addr, path.as_ref());

        copy(&mut stream, &mut file)?;

        Ok(())
    }
}
