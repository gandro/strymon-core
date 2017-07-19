use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::vec::Vec;

use abomonation::Abomonation;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::{Concat, Unary, Map as TimelyMap};
use timely::dataflow::channels::pact::Pipeline;
use timely_system::network::message::abomonate::NonStatic;

use keeper::model::{ClientQuery, QueryResponse};
use model::{KeeperQuery, KeeperResponse};

/// StateOperator
/// Can be constructed using StateOperatorBuilder.
pub struct StateOperator<G: Scope, DQ>
    where DQ: Abomonation + Any + Clone + NonStatic + Send // Type of outgoing stream.
{
    out_stream: Stream<G, QueryResponse<DQ>>,
}

impl<G: Scope, DQ> StateOperator<G, DQ>
    where DQ: Abomonation + Any + Clone + NonStatic + Send
{
    pub fn get_outgoing_responses_stream(&self) -> Stream<G, QueryResponse<DQ>> {
        self.out_stream.clone()
    }
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
enum UpdateOrQuery<U, Q>
    where U: Abomonation + Any + Clone + NonStatic,
          Q: Abomonation + Any + Clone + NonStatic
{
    Update(U),
    Query(Q),
}

/// TODO Document what all the type parameters stand for.
pub struct StateOperatorBuilder<'a, DS, DQ, DQ1, T: 'static, G: 'a + Scope>
    where DS: Abomonation + Any + Clone + NonStatic, // Type of data in incoming state stream.
          DQ: Abomonation + Any + Clone + NonStatic, // Type of data in incoming updates stream.
          DQ1: Abomonation + Any + Clone + NonStatic + Send // Type of data in outgoing stream.
{
    name: &'a str,
    in_state_stream: &'a Stream<G, DS>,
    in_query_stream: &'a Stream<G, ClientQuery<DQ>>,
    state_logic: Box<FnMut(Rc<RefCell<T>>, &DS) + 'static>,
    dump_state_logic: Box<FnMut(Rc<RefCell<T>>) -> Vec<DQ1> + 'static>,
    update_transform_logic: Box<FnMut(&DS) -> Vec<DQ1> + 'static>,
    query_logic: Option<Box<FnMut(Rc<RefCell<T>>, &DQ) -> Vec<DQ1> + 'static>>,
    state: Rc<RefCell<T>>,
}

impl<'a, DS, DQ, DQ1, T: 'static, G: 'a + Scope> StateOperatorBuilder<'a, DS, DQ, DQ1, T, G>
    where DS: Abomonation + Any + Clone + NonStatic, // Type of data in incoming state stream.
          DQ: Abomonation + Any + Clone + NonStatic, // Type of data in incoming updates stream.
          DQ1: Abomonation + Any + Clone + NonStatic + Send
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
              LDS: FnMut(Rc<RefCell<T>>) -> Vec<DQ1> + 'static,
              LUT: FnMut(&DS) -> Vec<DQ1> + 'static
    {
        StateOperatorBuilder {
            name: name,
            in_state_stream: state_stream,
            in_query_stream: query_stream,
            state_logic: Box::new(state_logic),
            dump_state_logic: Box::new(dump_state_logic),
            update_transform_logic: Box::new(update_transform_logic),
            query_logic: None,
            state: Rc::new(RefCell::new(state)),
        }
    }

    pub fn construct(self) -> StateOperator<G, DQ1> {
        let in_state = self.in_state_stream.map(|x| UpdateOrQuery::Update(x));
        let in_query = self.in_query_stream.map(|x| UpdateOrQuery::Query(x));
        let inputs = in_state.concat(&in_query);
        let state = self.state.clone();

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
                            let mut response = QueryResponse::broadcast();
                            response.append_tuples(&mut update_transform_logic(u)
                                                            .into_iter()
                                                            .map(|x| {
                                                                     KeeperResponse::Response(x)
                                                                 })
                                                            .collect());
                            response.add_tuple(KeeperResponse::BatchEnd);
                            response
                        }
                        UpdateOrQuery::Query(ref q) => {
                            match q.query() {
                                &KeeperQuery::StateRq(ref req) => {
                                    let mut response = QueryResponse::unicast(q, req.subscribe());
                                    if req.state() {
                                        response.append_tuples(
                                                & mut dump_state_logic(state.clone())
                                                    .into_iter()
                                                    .map(|x| KeeperResponse::Response(x))
                                                    .collect()
                                            );
                                        if req.subscribe() {
                                            response.add_tuple(KeeperResponse::BatchEnd);
                                        } else {
                                            response.add_tuple(KeeperResponse::ConnectionEnd);
                                        }
                                    }
                                    response
                                }
                                &KeeperQuery::Query(ref query) => {
                                    let mut response = QueryResponse::unicast(q, false);
                                    match &mut query_logic {
                                        &mut Some(ref mut logic) => {
                                            response.append_tuples(
                                                    & mut logic(state.clone(), query)
                                                        .into_iter()
                                                        .map(|x| KeeperResponse::Response(x))
                                                        .collect()
                                                );
                                            response.add_tuple(KeeperResponse::ConnectionEnd);
                                        }
                                        _ => {
                                            // Currently we are just dropping incorrectly
                                            // formatted queries so we do it here as well
                                            // (empty tuple list means no response is sent).
                                        }
                                    }
                                    response
                                }
                            }
                        }
                    };
                    output.session(&cap).give(response);
                }
            });
        });
        StateOperator { out_stream: outputs }
    }

    pub fn set_query_logic<LC>(mut self, query_logic: LC) -> Self
        where LC: FnMut(Rc<RefCell<T>>, &DQ) -> Vec<DQ1> + 'static
    {
        self.query_logic = Some(Box::new(query_logic));
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
