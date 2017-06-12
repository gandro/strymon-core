use std::any::Any;
use std::vec::Vec;
use abomonation::Abomonation;
use timely_system::network::message::abomonate::NonStatic;

#[derive(Clone, Debug, Abomonation)]
pub struct ClientQuery<D>
    where D: Abomonation + Any + Clone + NonStatic
{
    query: D,
    /// connection_id is valid only in the context of the same worker, so ClientQuery needs to know
    /// what worker_index it is binded to.
    connection_id: u64,
    /// worker_index is used to make sure this query is directed to outgoing stream on the same
    /// worker as it was created, and thus sent to the correct client.
    worker_index: usize,
}

impl<D> ClientQuery<D>
    where D: Abomonation + Any + Clone + NonStatic
{
    pub fn new(query: &D, connection_id: u64, worker_index: usize) -> Self {
        ClientQuery {
            query: query.clone(),
            connection_id: connection_id,
            worker_index: worker_index,
        }
    }

    pub fn query(&self) -> &D {
        &self.query
    }

    /// Method that creates response object.
    pub fn create_response<R>(&self) -> ClientQueryResponse<R>
        where R: Abomonation + Any + Clone + Send + NonStatic
    {
        ClientQueryResponse {
            response_tuples: Vec::new(),
            connection_id: self.connection_id,
            worker_index: self.worker_index,
        }
    }
}

/// This needs to be created with the ClientQuery that this is the response for.
#[derive(Clone, Debug, Abomonation)]
pub struct ClientQueryResponse<D>
    where D: Abomonation + Any + Clone + Send + NonStatic
{
    response_tuples: Vec<D>,
    connection_id: u64,
    worker_index: usize,
}

impl<D> ClientQueryResponse<D>
    where D: Abomonation + Any + Clone + Send + NonStatic
{
    pub fn add_tuple(&mut self, tuple: &D) {
        self.response_tuples.push(tuple.clone());
    }

    pub fn response_tuples(&self) -> &Vec<D> {
        &self.response_tuples
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }
}
