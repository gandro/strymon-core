use std::any::Any;
use std::vec::Vec;
use abomonation::Abomonation;
use timely_system::network::message::abomonate::NonStatic;

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
pub struct ClientQuery<D>
    where D: Abomonation + Any + Clone + NonStatic
{
    query: KeeperQuery<D>,
    client: Client,
}

impl<D> ClientQuery<D>
    where D: Abomonation + Any + Clone + NonStatic
{
    pub fn new(query: KeeperQuery<D>, connection_id: u64, worker_index: usize) -> Self {
        ClientQuery {
            query: query,
            client: Client::new(connection_id, worker_index),
        }
    }

    pub fn query(&self) -> &KeeperQuery<D> {
        &self.query
    }

    pub fn client_details(&self) -> &Client {
        &self.client
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum QueryResponseType {
    Broadcast { worker_idx: usize },
    Client { client: Client, subscribe: bool },
}

/// This needs to be created with the ClientQuery that this is the response for.
#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub struct QueryResponse<D>
    where D: Abomonation + Any + Clone + Send + NonStatic
{
    response_tuples: Vec<KeeperResponse<D>>,
    resp_type: QueryResponseType,
}

impl<D> QueryResponse<D>
    where D: Abomonation + Any + Clone + Send + NonStatic
{
    /// Creates a broadcast response. Such a message is to be sent to all subscribed clients, it is
    /// not tied to a specific one.
    pub fn broadcast(worker_idx: usize) -> Self {
        QueryResponse {
            response_tuples: Vec::new(),
            resp_type: QueryResponseType::Broadcast { worker_idx: worker_idx },
        }
    }

    pub fn unicast<Q>(query: &ClientQuery<Q>, subscribe_to_updates: bool) -> Self
        where Q: Abomonation + Any + Clone + NonStatic
    {
        QueryResponse {
            response_tuples: Vec::new(),
            resp_type: QueryResponseType::Client {
                client: query.client.clone(),
                subscribe: subscribe_to_updates,
            },
        }
    }

    pub fn add_tuple(&mut self, tuple: KeeperResponse<D>) {
        self.response_tuples.push(tuple)
    }

    pub fn append_tuples(&mut self, tuples: &mut Vec<KeeperResponse<D>>) {
        self.response_tuples.append(tuples)
    }

    pub fn response_tuples(&self) -> &Vec<KeeperResponse<D>> {
        &self.response_tuples
    }

    pub fn client_details(&self) -> Option<&Client> {
        match &self.resp_type {
            &QueryResponseType::Broadcast { .. } => None,
            &QueryResponseType::Client { ref client, .. } => Some(client),
        }
    }

    pub fn is_broadcast(&self) -> bool {
        match &self.resp_type {
            &QueryResponseType::Broadcast { .. } => true,
            _ => false,
        }
    }

    pub fn response_type(&self) -> &QueryResponseType {
        &self.resp_type
    }

    /// Returns the value that should be used with `exchange` operator to route it to the right
    /// worker for sending out to client.
    pub fn route_to(&self) -> u64 {
        (match &self.resp_type {
             &QueryResponseType::Broadcast { worker_idx } => worker_idx,
             &QueryResponseType::Client { ref client, .. } => client.worker_index(),
         }) as u64
    }
}
