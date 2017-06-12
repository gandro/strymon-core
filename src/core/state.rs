use std::cell::RefCell;
use std::rc::Rc;
use std::vec::Vec;

use timely;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::{Unary, InputHandle, OutputHandle, Notificator};
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pushers::tee::Tee;

/// StateOperator
/// Can be constructed using StateOperatorBuilder.
pub struct StateOperator<T: 'static, G: Scope, DS, DC>
    where DS: timely::Data,
          DC: timely::Data
{
    out_state_stream: Stream<G, DS>,
    out_clients_stream: Stream<G, DC>,
    //TODO delete state if we end up not using it.
    #[allow(dead_code)]
    state: Rc<RefCell<T>>,
}

impl<T: 'static, G: Scope, DS, DC> StateOperator<T, G, DS, DC>
    where DS: timely::Data,
          DC: timely::Data
{
    pub fn get_outgoing_state_stream(&self) -> Stream<G, DS> {
        self.out_state_stream.clone()
    }
    pub fn get_outgoing_clients_stream(&self) -> Stream<G, DC> {
        self.out_clients_stream.clone()
    }
}

/// TODO Document what all the type parameters stand for.
///
/// Methods of this builder will panic if any of the invariants it assumes are not kept.
/// Invariants:
///  - call to construct can happen only when both client and state streams have an unary operator
///  defined on them (either stream or notify)
///  - call to (state|clients)_unary_(stream|notify) can happen only once for state and once for
///  clients
pub struct StateOperatorBuilder<T: 'static, G: Scope, DS, DS1, DC, DC1>
    where DS: timely::Data,
          DS1: timely::Data,
          DC: timely::Data,
          DC1: timely::Data
{
    in_state_stream: Stream<G, DS>,
    in_clients_stream: Stream<G, DC>,
    out_state_stream: Option<Stream<G, DS1>>,
    out_clients_stream: Option<Stream<G, DC1>>,
    state: Rc<RefCell<T>>,
}

impl<T: 'static, G: Scope, DS, DS1, DC, DC1> StateOperatorBuilder<T, G, DS, DS1, DC, DC1>
    where DS: timely::Data,
          DS1: timely::Data,
          DC: timely::Data,
          DC1: timely::Data
{
    pub fn new(state: T, state_stream: &Stream<G, DS>, clients_stream: &Stream<G, DC>) -> Self {
        StateOperatorBuilder {
            in_state_stream: state_stream.clone(),
            in_clients_stream: clients_stream.clone(),
            state: Rc::new(RefCell::new(state)),
            out_state_stream: None,
            out_clients_stream: None,
        }
    }

    pub fn construct(self) -> StateOperator<T, G, DS1, DC1> {
        assert!(self.out_state_stream.is_some() && self.out_clients_stream.is_some());
        StateOperator {
            out_state_stream: self.out_state_stream.unwrap(),
            out_clients_stream: self.out_clients_stream.unwrap(),
            state: self.state,
        }
    }

    pub fn state_unary_stream<P, L>(mut self, pact: P, name: &str, mut logic: L) -> Self
        where L: FnMut(Rc<RefCell<T>>,
                       &mut InputHandle<G::Timestamp, DS>,
                       &mut OutputHandle<G::Timestamp, DS1, Tee<G::Timestamp, DS1>>) + 'static,
              P: ParallelizationContract<G::Timestamp, DS>
    {
        assert!(self.out_state_stream.is_none());
        let state = self.state.clone();
        let out_stream = self.in_state_stream.unary_stream(pact, name, move |input, output| {
            logic(state.clone(), input, output)
        });
        self.out_state_stream = Some(out_stream);
        self
    }

    pub fn state_unary_notify<P, L>(mut self,
                                    pact: P,
                                    name: &str,
                                    init: Vec<G::Timestamp>,
                                    mut logic: L)
                                    -> Self
        where L: FnMut(Rc<RefCell<T>>,
                       &mut InputHandle<G::Timestamp, DS>,
                       &mut OutputHandle<G::Timestamp, DS1, Tee<G::Timestamp, DS1>>,
                       &mut Notificator<G::Timestamp>) + 'static,
              P: ParallelizationContract<G::Timestamp, DS>
    {
        assert!(self.out_state_stream.is_none());
        let state = self.state.clone();
        let out_stream =
            self.in_state_stream.unary_notify(pact,
                                              name,
                                              init,
                                              move |input, output, notificator| {
                                                  logic(state.clone(), input, output, notificator)
                                              });
        self.out_state_stream = Some(out_stream);
        self
    }

    pub fn clients_unary_stream<P, L>(mut self, pact: P, name: &str, mut logic: L) -> Self
        where L: FnMut(Rc<RefCell<T>>,
                       &mut InputHandle<G::Timestamp, DC>,
                       &mut OutputHandle<G::Timestamp, DC1, Tee<G::Timestamp, DC1>>) + 'static,
              P: ParallelizationContract<G::Timestamp, DC>
    {
        assert!(self.out_clients_stream.is_none());
        let state = self.state.clone();
        let out_stream = self.in_clients_stream.unary_stream(pact, name, move |input, output| {
            logic(state.clone(), input, output)
        });
        self.out_clients_stream = Some(out_stream);
        self
    }

    pub fn clients_unary_notify<P, L>(mut self,
                                      pact: P,
                                      name: &str,
                                      init: Vec<G::Timestamp>,
                                      mut logic: L)
                                      -> Self
        where L: FnMut(Rc<RefCell<T>>,
                       &mut InputHandle<G::Timestamp, DC>,
                       &mut OutputHandle<G::Timestamp, DC1, Tee<G::Timestamp, DC1>>,
                       &mut Notificator<G::Timestamp>) + 'static,
              P: ParallelizationContract<G::Timestamp, DC>
    {
        assert!(self.out_clients_stream.is_none());
        let state = self.state.clone();
        let out_stream = self.in_clients_stream
            .unary_notify(pact, name, init, move |input, output, notificator| {
                logic(state.clone(), input, output, notificator)
            });
        self.out_clients_stream = Some(out_stream);
        self
    }
}

#[cfg(test)]
mod tests {
    use timely;
    use timely::dataflow::operators::{ToStream, Inspect};
    use timely::dataflow::channels::pact::Pipeline;

    use core::model::ClientQuery;
    use super::StateOperatorBuilder;

    /// This tests if the API works. There are no asserts in here, but if it doesn't compile or
    /// hangs it means that the test fails.
    /// print statements in inspect can be helpful for debugging (when run with
    /// `cargo test -- --nocapture`).
    #[test]
    fn test() {
        timely::example(|scope| {
            let empty_query = String::new();
            let client_stream = vec![ClientQuery::new(&empty_query, 0, 0),
                                     ClientQuery::new(&empty_query, 0, 0)]
                    .to_stream(scope);
            let input_stream = (0..10).to_stream(scope);

            let state_operator = StateOperatorBuilder::new(0 as u64, &input_stream, &client_stream)
                .state_unary_stream(Pipeline, "StateBuilder", |state, input, output| {
                    input.for_each(|cap, data| {
                        let mut st = state.borrow_mut();
                        let mut session = output.session(&cap);
                        for &d in data.iter() {
                            *st += d;
                            session.give(d);
                        }
                    });
                })
                .clients_unary_stream(Pipeline, "ClientAnswerer", |state, input, output| {
                    input.for_each(|cap, data| {
                        let st = state.borrow();
                        let mut session = output.session(&cap);
                        for d in data.iter() {
                            let mut resp = d.create_response();
                            resp.add_tuple(&((*st).to_string()));
                            session.give(resp);
                        }
                    });
                })
                .construct();

            state_operator.get_outgoing_state_stream().inspect(|x| println!("State: {}", x));
            state_operator.get_outgoing_clients_stream().inspect(|x| println!("Clients: {:?}", x));
        });
    }
}
