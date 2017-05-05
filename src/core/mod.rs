use abomonation::Abomonation;
use timely_system::network::reqrep::Request;

#[derive(Clone, Debug)]
pub struct Query(pub String);

#[derive(Clone, Debug)]
pub struct Response(pub String);

unsafe_abomonate!(Query);
unsafe_abomonate!(Response);

impl Request for Query {
    type Success = Response; 
    // TODO!!: add meaningful error
    type Error = ();

    fn name() -> &'static str {
        "Query"
    }
}

mod endpoints;

pub use self::endpoints::{Connector, ClientQuery, ClientQueryResponse};
