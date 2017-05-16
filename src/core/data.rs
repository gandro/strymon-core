
#[derive(Clone, Debug, Abomonation)]
pub struct ClientQuery {
    query: String,
    /// connection_id is valid only in the context of the same worker, so ClientQuery needs to know
    /// what worker_index it is binded to.
    connection_id: u64,
    /// worker_index is used to make sure this query is directed to outgoing stream on the same
    /// worker as it was created, and thus sent to the correct client.
    worker_index: usize,
}

impl ClientQuery {
    pub fn new(query: &str, connection_id: u64, worker_index: usize) -> Self {
        ClientQuery {
            query: query.to_string(),
            connection_id: connection_id,
            worker_index: worker_index,
        }
    }

    pub fn query(&self) -> String {
        self.query.to_string()
    }

    /// Method that creates response object.
    pub fn create_response(&self, response: &str) -> ClientQueryResponse {
        ClientQueryResponse {
            response: response.to_string(),
            connection_id: self.connection_id,
            worker_index: self.worker_index,
        }
    }
}

/// This needs to be created with the ClientQuery that this is the response for.
#[derive(Clone, Debug, Abomonation)]
pub struct ClientQueryResponse {
    response: String,
    connection_id: u64,
    worker_index: usize,
}

impl ClientQueryResponse {
    pub fn new(response: &str, cq: &ClientQuery) -> Self {
        ClientQueryResponse {
            response: response.to_string(),
            connection_id: cq.connection_id,
            worker_index: cq.worker_index,
        }
    }

    pub fn response(&self) -> String {
        self.response.to_string()
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }

}
