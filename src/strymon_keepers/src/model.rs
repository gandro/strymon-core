use abomonation::Abomonation;

#[derive(Clone, Debug, Abomonation, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Abomonation, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeeperQuery<Q> {
    StateRq(StateRequest),
    Query(Q),
}

#[derive(Clone, Debug, Abomonation, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeeperResponse<R> {
    Response(R),
    BatchEnd,
    ConnectionEnd,
}

/// Empty query type for Keepers that do not support adhoc queries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, )]
pub enum EmptyQT {}

unsafe_abomonate!(EmptyQT);
