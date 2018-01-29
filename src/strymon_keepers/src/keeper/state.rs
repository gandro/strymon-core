use std::cell::RefCell;
use std::rc::Rc;
use std::vec::Vec;
use std::collections::HashMap;

use timely::{Data, ExchangeData};
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::{Concat, Exchange, Unary, Map as TimelyMap};
use timely::dataflow::channels::pact::Pipeline;

use keeper::model::{ClientQuery, QueryResponse};
use model::{KeeperQuery, KeeperResponse};

/// StateOperator
/// Can be constructed using StateOperatorBuilder.
pub struct StateOperator<G: Scope, DQ> {
    out_stream: Stream<G, QueryResponse<DQ>>,
}

impl<G: Scope, DQ> StateOperator<G, DQ> where DQ: Data {
    pub fn get_outgoing_responses_stream(&self) -> Stream<G, QueryResponse<DQ>> {
        self.out_stream.clone()
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
enum UpdateOrQuery<U, Q> {
    Update(U),
    Query(Q),
}

/// TODO Document what all the type parameters stand for.
pub struct StateOperatorBuilder<'a, DS: 'a, DQ: 'a, DO, T: 'static, G: 'a + Scope> {
    name: &'a str,
    in_state_stream: &'a Stream<G, DS>,
    in_query_stream: &'a Stream<G, ClientQuery<DQ>>,
    state_logic: Box<FnMut(Rc<RefCell<T>>, &DS) + 'static>,
    dump_state_logic: Box<FnMut(Rc<RefCell<T>>) -> Vec<DO> + 'static>,
    update_transform_logic: Box<FnMut(&DS) -> Vec<DO> + 'static>,
    query_logic: Option<Box<FnMut(Rc<RefCell<T>>, &DQ) -> Vec<DO> + 'static>>,
    query_distribution_logic: Option<Box<FnMut(&KeeperQuery<DQ>) -> Vec<usize> + 'static>>,
    response_merge_logic: Option<Box<FnMut(&KeeperQuery<DQ>, Vec<Vec<KeeperResponse<DO>>>)
                                           -> Vec<KeeperResponse<DO>> + 'static>>,
    state: Rc<RefCell<T>>,
}

impl<'a, DS, DQ, DO, T: 'static, G: 'a + Scope> StateOperatorBuilder<'a, DS, DQ, DO, T, G>
    where DS: ExchangeData, // Type of data in incoming updates stream.
          DQ: ExchangeData, // Type of data in incoming queries stream.
          DO: ExchangeData // Type of data in outgoing stream.
{
    pub fn new<LS, LUT, LDS>(name: &'a str,
                             state: T,
                             state_stream: &'a Stream<G, DS>,
                             query_stream: &'a Stream<G, ClientQuery<DQ>>,
                             state_logic: LS,
                             update_transform_logic: LUT,
                             dump_state_logic: LDS)
                             -> Self
        where LS: FnMut(Rc<RefCell<T>>, &DS) + 'static,
              LDS: FnMut(Rc<RefCell<T>>) -> Vec<DO> + 'static,
              LUT: FnMut(&DS) -> Vec<DO> + 'static
    {
        StateOperatorBuilder {
            name: name,
            in_state_stream: state_stream,
            in_query_stream: query_stream,
            state_logic: Box::new(state_logic),
            dump_state_logic: Box::new(dump_state_logic),
            update_transform_logic: Box::new(update_transform_logic),
            query_logic: None,
            query_distribution_logic: None,
            response_merge_logic: None,
            state: Rc::new(RefCell::new(state)),
        }
    }

    pub fn construct(mut self) -> StateOperator<G, DO> {
        let mut in_query_stream = self.in_query_stream.clone();

        // Distribute queries to correct workers (needed in case of multi-worker Keeper).
        if let Some(mut logic) = self.query_distribution_logic.take() {
            in_query_stream =
                in_query_stream.unary_stream(Pipeline, "DistributeQueries", move |input, output| {
                        input.for_each(|cap, data| for query in data.iter() {
                                           let mut query = query.clone();
                                           let workers = &mut logic(query.query());
                                           query.set_copies_no(workers.len());
                                           output.session(&cap)
                                               .give_iterator(workers.iter().map(|&idx| {
                                                                                     (idx,
                                                                                  query.clone())
                                                                                 }));
                                       });
                    })
                    .exchange(|&(idx, _)| idx as u64)
                    .map(|(_, query)| query);
        }

        let in_query = in_query_stream.map(|x| UpdateOrQuery::Query(x));
        let in_state = self.in_state_stream.map(|x| UpdateOrQuery::Update(x));
        let inputs = in_state.concat(&in_query);
        let state = self.state.clone();

        let worker_idx = self.in_state_stream.scope().index();
        let mut state_logic = self.state_logic;
        let mut dump_state_logic = self.dump_state_logic;
        let mut update_transform_logic = self.update_transform_logic;
        let mut query_logic = self.query_logic;

        let outputs = inputs.unary_stream(Pipeline, self.name, move |input, output| {
            input.for_each(|cap, data| {
                for msg in data.iter() {
                    let mut msg = msg.clone();
                    let response = match msg {
                        UpdateOrQuery::Update(ref mut u) => {
                            // Update the state.
                            state_logic(state.clone(), u);
                            // Produce broadcast message to all clients that subscribed to
                            // updates.
                            let mut response = QueryResponse::broadcast(worker_idx);
                            response.append_tuples(&mut update_transform_logic(u)
                                                            .into_iter()
                                                            .map(|x| {
                                                                     KeeperResponse::Response(x)
                                                                 })
                                                            .collect());
                            (None, response)
                        }
                        UpdateOrQuery::Query(ref q) => {
                            match q.query() {
                                &KeeperQuery::StateRq(ref req) => {
                                    let mut response =
                                        QueryResponse::unicast(q, req.subscribe(), worker_idx);
                                    if req.state() {
                                        response.append_tuples(
                                                & mut dump_state_logic(state.clone())
                                                    .into_iter()
                                                    .map(|x| KeeperResponse::Response(x))
                                                    .collect()
                                            );
                                    }
                                    (Some(q.query().clone()), response)
                                }
                                &KeeperQuery::Query(ref query) => {
                                    let mut response = QueryResponse::unicast(q, false, worker_idx);
                                    match &mut query_logic {
                                        &mut Some(ref mut logic) => {
                                            response.append_tuples(
                                                    & mut logic(state.clone(), query)
                                                        .into_iter()
                                                        .map(|x| KeeperResponse::Response(x))
                                                        .collect()
                                                );
                                        }
                                        _ => {
                                            // Currently we are just dropping incorrectly
                                            // formatted queries so we do it here as well
                                            // (empty tuple list means no response is sent).
                                        }
                                    }
                                    (Some(q.query().clone()), response)
                                }
                            }
                        }
                    };
                    output.session(&cap).give(response);
                }
            });
        });
        let outputs = if let Some(mut logic) = self.response_merge_logic.take() {
            let broadcasts = outputs.flat_map(|(q, x)| if q.is_none() { vec![x] } else { vec![] });
            let unicasts = outputs.flat_map(|(q, x)| {
                                assert!((q.is_some() && !x.is_broadcast()) ||
                                        (q.is_none() && x.is_broadcast()));
                                match x {
                                    QueryResponse::Broadcast(_) => vec![],
                                    QueryResponse::Unicast(uni) => {
                                        match q {
                                          Some(q) => vec![(q, uni)],
                                          None => unreachable!(),
                                        }
                                    },
                                }
                            })
                            // Same timestamp = same worker so we can collect them.
                            .exchange(|&(_, ref x)| x.timestamp());

            let mut pending_responses = HashMap::<u64, Vec<Vec<KeeperResponse<DO>>>>::new();

            let unicasts =
                unicasts.unary_stream(Pipeline, "MergeResponses", move |input, output| {
                    input.for_each(|cap, data| for &(ref q, ref msg) in data.iter() {
                                       let curr_len = {
                            let resp_vec = pending_responses.entry(msg.timestamp())
                                .or_insert(Vec::new());
                            resp_vec.push(msg.response_tuples().clone());
                            resp_vec.len()
                        };
                                       if curr_len == msg.copies_no() {
                                           let resp_vec =
                            pending_responses.remove(&msg.timestamp()).expect("Invariant broken");
                                           let mut resp = msg.empty_clone();
                                           resp.append_tuples(&mut logic(q, resp_vec));
                                           output.session(&cap).give(resp);
                                       }
                                   });
                });

            unicasts.map(|x| QueryResponse::Unicast(x)).concat(&broadcasts)
        } else {
            outputs.map(|(_, resp)| resp)
        };
        StateOperator { out_stream: outputs }
    }

    pub fn set_query_logic<QL>(mut self, query_logic: QL) -> Self
        where QL: FnMut(Rc<RefCell<T>>, &DQ) -> Vec<DO> + 'static
    {
        self.query_logic = Some(Box::new(query_logic));
        self
    }

    pub fn set_query_distribution_logic<QDL>(mut self, query_distribution_logic: QDL) -> Self
        where QDL: FnMut(&KeeperQuery<DQ>) -> Vec<usize> + 'static
    {
        self.query_distribution_logic = Some(Box::new(query_distribution_logic));
        self
    }

    pub fn set_response_merge_logic<RML>(mut self, response_merge_logic: RML) -> Self
        where RML: FnMut(&KeeperQuery<DQ>, Vec<Vec<KeeperResponse<DO>>>) -> Vec<KeeperResponse<DO>> + 'static
    {
        self.response_merge_logic = Some(Box::new(response_merge_logic));
        self
    }
}

#[cfg(test)]
mod tests {
    use timely;
    use timely::dataflow::operators::{ToStream, Inspect};

    use keeper::model::ClientQuery;
    use model::{StateRequest, KeeperQuery};
    use super::StateOperatorBuilder;

    /// This tests if the API works. There are no asserts in here, but if it doesn't compile or
    /// hangs it means that the test fails.
    /// print statements in inspect can be helpful for debugging (when run with
    /// `cargo test -- --nocapture`).
    #[test]
    fn test() {
        timely::example(|scope| {
            let client_stream = vec![
                     ClientQuery::new(KeeperQuery::StateRq(StateRequest::StateAndUpdates), 0, 0),
                     ClientQuery::new(KeeperQuery::StateRq(StateRequest::JustState), 0, 0),
                     ClientQuery::new(KeeperQuery::Query(String::new()), 0, 0)]
                    .to_stream(scope);
            let input_stream = (0..10).to_stream(scope);

            let state_operator =
                StateOperatorBuilder::new("StateTest",
                                          0 as u64,
                                          &input_stream,
                                          &client_stream,
                                          |state, d| {
                                              // State logic.
                                              let mut state = state.borrow_mut();
                                              *state += *d;
                                          },
                                          |update| vec![format!("Update: '{:?}'", update)],
                                          |state| {
                                              // Dump state logic.
                                              vec![format!("Dump state: {:?}", state.borrow())]
                                          })
                        .set_query_logic(|_, query| {
                                             vec![format!("Query logic response: {:?}", query)]
                                         })
                        .construct();

            state_operator.get_outgoing_responses_stream().inspect(|x| println!("Out: '{:?}'", x));
        });
    }
}
