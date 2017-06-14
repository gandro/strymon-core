use std::any::Any;
use abomonation::Abomonation;
use timely_system::network::message::abomonate::NonStatic;

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum StateRequest {
    JustState,
    JustUpdates,
    StateAndUpdates,
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
