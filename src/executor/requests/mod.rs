use abomonation::Abomonation;

use model::*;
use network::reqrep::Request;

#[derive(Debug, Clone)]
pub struct SpawnQuery {
    pub query: Query,
    pub hostlist: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum SpawnError {
    InvalidRequest,
    FetchFailed,
    ExecFailed,
}

impl Request for SpawnQuery {
    type Success = ();
    type Error = SpawnError;

    fn name() -> &'static str {
        "SpawnQuery"
    }
}

unsafe_abomonate!(SpawnQuery: query, hostlist);
unsafe_abomonate!(SpawnError);
