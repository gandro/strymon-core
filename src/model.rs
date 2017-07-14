use std::any::Any;
use abomonation::Abomonation;
use timely_system::network::message::abomonate::NonStatic;

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum StateRequest {
    JustState,
    JustUpdates,
    StateAndUpdates,
}

impl StateRequest {
    pub fn subscribe(&self) -> bool {
        match self {
            &StateRequest::JustState => false,
            _ => true,
        }
    }

    pub fn state(&self) -> bool {
        match self {
            &StateRequest::JustUpdates => false,
            _ => true,
        }
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum KeeperQuery<Q> 
    where Q: Abomonation + Any + Clone + NonStatic
{
    StateRq(StateRequest),
    Query(Q),
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum KeeperResponse<R>
    where R: Abomonation + Any + Clone + Send + NonStatic
{
    Response(R),
    BatchEnd,
    ConnectionEnd,
}

/// Empty query type for Keepers that do not support adhoc queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmptyQT {}

unsafe_abomonate!(EmptyQT);
