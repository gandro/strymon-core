use std::vec::Vec;
use std::time::Instant;

use model::{KeeperQuery, KeeperResponse};

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub struct Client {
    /// connection_id is valid only in the context of the same worker, so ClientQuery needs to know
    /// what worker_index it is binded to.
    connection_id: u64,
    /// worker_index is used to make sure this query is directed to outgoing stream on the same
    /// worker as it was created, and thus sent to the correct client.
    worker_index: usize,
}

impl Client {
    fn new(connection_id: u64, worker_index: usize) -> Self {
        Client {
            connection_id: connection_id,
            worker_index: worker_index,
        }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub struct ClientQuery<D> {
    query: KeeperQuery<D>,
    client: Client,
    copies: usize,
    timestamp: u64,
}

impl<D> ClientQuery<D>
{
    pub fn new(query: KeeperQuery<D>, connection_id: u64, worker_index: usize) -> Self {
        lazy_static! {
            static ref ZERO_TIME: Instant = Instant::now();
        }
        let duration = ZERO_TIME.elapsed();
        ClientQuery {
            query: query,
            client: Client::new(connection_id, worker_index),
            copies: 1,
            timestamp: duration.as_secs() * (1e9 as u64) + duration.subsec_nanos() as u64,
        }
    }

    pub fn query(&self) -> &KeeperQuery<D> {
        &self.query
    }

    pub fn client_details(&self) -> &Client {
        &self.client
    }

    pub fn set_copies_no(&mut self, copies: usize) {
        self.copies = copies;
    }
}

/// This needs to be created with the ClientQuery that this is the response for.
#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum QueryResponse<D> {
    Broadcast(Broadcast<D>),
    Unicast(Unicast<D>),
}

impl<D> QueryResponse<D> {
    /// Creates a broadcast response. Such a message is to be sent to all subscribed clients, it is
    /// not tied to a specific one.
    pub fn broadcast(source_worker_idx: usize) -> Self {
        QueryResponse::Broadcast(Broadcast {
                                     response_tuples: Vec::new(),
                                     source_worker_idx: source_worker_idx,
                                     target_worker_idx: 0,
                                 })
    }

    pub fn unicast<Q>(query: &ClientQuery<Q>, subscribe_to_updates: bool, worker_idx: usize) -> Self
    {
        QueryResponse::Unicast(Unicast {
                                   response_tuples: Vec::new(),
                                   client: query.client.clone(),
                                   subscribe: subscribe_to_updates,
                                   source_worker_idx: worker_idx,
                                   copies: query.copies,
                                   timestamp: query.timestamp,
                               })
    }

    /// Returns the value that should be used with `exchange` operator to route it to the right
    /// worker for sending out to client.
    pub fn route_to(&self) -> u64 {
        (match self {
             &QueryResponse::Broadcast(ref broad) => broad.target_worker_idx,
             &QueryResponse::Unicast(ref uni) => uni.client_details().worker_index(),
         }) as u64
    }

    pub fn is_broadcast(&self) -> bool {
        match self {
            &QueryResponse::Broadcast(_) => true,
            _ => false,
        }
    }

    pub fn add_tuple(&mut self, tuple: KeeperResponse<D>) {
        match self {
            &mut QueryResponse::Broadcast(ref mut msg) => msg.add_tuple(tuple),
            &mut QueryResponse::Unicast(ref mut msg) => msg.add_tuple(tuple),
        }
    }

    pub fn append_tuples(&mut self, tuples: &mut Vec<KeeperResponse<D>>) {
        match self {
            &mut QueryResponse::Broadcast(ref mut msg) => msg.response_tuples.append(tuples),
            &mut QueryResponse::Unicast(ref mut msg) => msg.response_tuples.append(tuples),
        }
    }

    pub fn response_tuples(&self) -> &Vec<KeeperResponse<D>> {
        match self {
            &QueryResponse::Broadcast(ref msg) => &msg.response_tuples,
            &QueryResponse::Unicast(ref msg) => &msg.response_tuples,
        }
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub struct Broadcast<D> {
    response_tuples: Vec<KeeperResponse<D>>,
    source_worker_idx: usize,
    target_worker_idx: usize,
}

impl<D> Broadcast<D> {
    pub fn add_tuple(&mut self, tuple: KeeperResponse<D>) {
        self.response_tuples.push(tuple)
    }

    pub fn append_tuples(&mut self, tuples: &mut Vec<KeeperResponse<D>>) {
        self.response_tuples.append(tuples)
    }

    pub fn response_tuples(&self) -> &Vec<KeeperResponse<D>> {
        &self.response_tuples
    }

    pub fn set_target_worker_idx(&mut self, target_worker_idx: usize) {
        self.target_worker_idx = target_worker_idx;
    }

    pub fn source_worker_idx(&self) -> usize {
        self.source_worker_idx
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub struct Unicast<D> {
    response_tuples: Vec<KeeperResponse<D>>,
    client: Client,
    subscribe: bool,
    source_worker_idx: usize,
    /// `copies` and `hash` used for multi-worker Keepers
    copies: usize,
    timestamp: u64,
}

impl<D> Unicast<D> {
    pub fn add_tuple(&mut self, tuple: KeeperResponse<D>) {
        self.response_tuples.push(tuple)
    }

    pub fn append_tuples(&mut self, tuples: &mut Vec<KeeperResponse<D>>) {
        self.response_tuples.append(tuples)
    }

    pub fn response_tuples(&self) -> &Vec<KeeperResponse<D>> {
        &self.response_tuples
    }

    pub fn client_details(&self) -> &Client {
        &self.client
    }

    pub fn subscribe(&self) -> bool {
        self.subscribe
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn copies_no(&self) -> usize {
        self.copies
    }

    pub fn source_worker_idx(&self) -> usize {
        self.source_worker_idx
    }

    pub fn empty_clone(&self) -> Self {
        Unicast {
            response_tuples: Vec::new(),
            client: self.client.clone(),
            subscribe: self.subscribe,
            source_worker_idx: self.source_worker_idx,
            copies: self.copies,
            timestamp: self.timestamp,
        }
    }
}
