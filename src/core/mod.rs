use timely_system::network::reqrep::Request;

// Abomonation doesn't work with structs with anonymous fields unfortunately.
#[derive(Clone, Debug, Abomonation)]
pub struct Query {
    pub text: String,
}

impl Query {
    pub fn new(text: &str) -> Self {
        Query {
            text: text.to_string(),
        }
    }
}

#[derive(Clone, Debug, Abomonation)]
pub struct Response {
    pub text: String,
}

impl Response {
    pub fn new(text: &str) -> Self {
        Response {
            text: text.to_string(),
        }
    }
}

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

mod state;
pub use self::state::{StateOperator, StateOperatorBuilder};
