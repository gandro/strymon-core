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
pub struct ResponseTuple {
    pub text: String,
}

impl ResponseTuple {
    pub fn new(text: &str) -> Self {
        ResponseTuple {
            text: text.to_string(),
        }
    }
}

impl Request for Query {
    type Success = ResponseTuple;
    // TODO!!: add meaningful error
    type Error = ();

    fn name() -> &'static str {
        "Query"
    }
}

mod endpoints;

pub use self::endpoints::Connector;

mod state;
pub use self::state::{StateOperator, StateOperatorBuilder};

pub mod messenger;
pub mod data;
