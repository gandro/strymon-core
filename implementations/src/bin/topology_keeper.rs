//! It assumes to be run as one worker.
//! # Data parsed
//!
//! Since incomming data has no other way to identify nodes than name, the names are unique.
//!
//! add host                AH;<name>;<switch_name> (host has to be connected to a switch)
//! add switch              AS;<name>
//! remove connection       RC;<switch_name1>;<switch_name2>
//! add bidir connection    AC;<switch_name1>;<switch_name2>;<weight>
//!
//! # Queries
//!
//! get all hosts           GAH
//! get all switches        GAS
//! get all connections     GAC
//! is connected            IC;<name1>;<name1>
//!
//! # Output
//!
//! H;<name>;<switch_name>
//! S;<name>
//! C;<switch_name1>;<switch_name2>;<weight>;<bool>
//!
//! (bool is either 0 or 1)
#[macro_use]
extern crate abomonation_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate abomonation;
extern crate env_logger;
extern crate timely;
extern crate timely_keepers;
extern crate timely_query;
extern crate regex;
extern crate model as topology_model;

use std::fmt;
use std::error;
use std::convert::From;
use std::collections::HashMap;

use regex::Regex;
use topology_model::topology::{Connection, Host, Switch, NodeId, LinkId, LinkWeight, Topology};
use timely_keepers::keeper::{Connector, StateOperatorBuilder};
use timely_keepers::model::{KeeperQuery, KeeperResponse};
use timely::dataflow::channels::message::Content;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Map, UnorderedInput, Inspect};

#[derive(Abomonation, Clone, Debug, PartialEq)]
enum UpdateType {
    AddHost(String, String),
    AddSwitch(String),
    RemoveConnection(String, String),
    AddBidirConnection(String, String, LinkWeight),
}

/// Input:                           Output:
/// AH;<name>;<switch_name>          AddHost(<name>, <switch_name>)
/// AS;<name>                        AddSwitch(<name>)
/// RC;<switch_name>;<switch_name>   RemoveConnection(<switch_name>, <switch_name>)
/// AC;<sname>;<sname>;<weight>      AddBidirConnection(<sname>, <sname>, <weight>)
fn parse_update(update: &str) -> Option<UpdateType> {
    lazy_static! {
        static ref RE_QUERY: Regex = Regex::new("^(AH|AS|RC|AC)((?:;[A-Za-z_0-9]+)+)$").unwrap();
    }
    let caps = match RE_QUERY.captures(update) {
        Some(caps) => caps,
        None => return None,
    };

    if caps.len() < 3 {
        return None;
    }

    let action = match caps.get(1) {
        Some(a) => a.as_str(),
        None => return None,
    };

    match action {
        "AH" => {
            let args = match caps.get(2) {
                Some(args) => args,
                None => return None,
            };
            let args = &args.as_str()[1..].split(';').collect::<Vec<_>>();
            if args.len() != 2 {
                return None;
            }
            Some(UpdateType::AddHost(args[0].to_string(), args[1].to_string()))
        }
        "AS" => {
            let args = match caps.get(2) {
                Some(args) => args,
                None => return None,
            };
            let args = &args.as_str()[1..].split(';').collect::<Vec<_>>();
            if args.len() != 1 {
                return None;
            }
            Some(UpdateType::AddSwitch(args[0].to_string()))
        }
        "RC" => {
            let args = match caps.get(2) {
                Some(args) => args,
                None => return None,
            };
            let args = &args.as_str()[1..].split(';').collect::<Vec<_>>();
            if args.len() != 2 {
                return None;
            }
            Some(UpdateType::RemoveConnection(args[0].to_string(), args[1].to_string()))
        }
        "AC" => {
            let args = match caps.get(2) {
                Some(args) => args,
                None => return None,
            };
            let args = &args.as_str()[1..].split(';').collect::<Vec<_>>();
            if args.len() != 3 {
                return None;
            }
            let weight = match args[2].parse::<LinkWeight>() {
                Ok(w) => w,
                Err(_) => return None,
            };
            Some(UpdateType::AddBidirConnection(args[0].to_string(), args[1].to_string(), weight))
        }
        _ => None,
    }
}

#[derive(Debug, PartialEq)]
enum QueryType {
    GetAllHosts,
    GetAllSwitches,
    GetAllConnections,
    IsConnected(String, String),
}

/// Input:              Output:
/// GAH                 GetAllHosts
/// GAS                 GetAllSwitches
/// GAC                 GetAllConnections
/// IC;<name>;<name>    IsConnected(<switch_name>, <switch_name>)
fn parse_query(update: &str) -> Option<QueryType> {
    lazy_static! {
        static ref RE_QUERY: Regex = Regex::new("^(GAH|GAS|GAC|IC)((?:;[a-z_0-9]+)+)?$").unwrap();
    }
    let caps = match RE_QUERY.captures(update) {
        Some(caps) => caps,
        None => return None,
    };

    if caps.len() < 2 {
        return None;
    }

    let action = match caps.get(1) {
        Some(a) => a.as_str(),
        None => return None,
    };

    match action {
        "GAH" => {
            if caps.get(2).is_some() {
                return None;
            }
            Some(QueryType::GetAllHosts)
        }
        "GAS" => {
            if caps.get(2).is_some() {
                return None;
            }
            Some(QueryType::GetAllSwitches)
        }
        "GAC" => {
            if caps.get(2).is_some() {
                return None;
            }
            Some(QueryType::GetAllConnections)
        }
        "IC" => {
            let args = match caps.get(2) {
                Some(args) => args,
                None => return None,
            };
            let args = &args.as_str()[1..].split(';').collect::<Vec<_>>();
            if args.len() != 2 {
                return None;
            }
            Some(QueryType::IsConnected(args[0].to_string(), args[1].to_string()))
        }
        _ => None,
    }
}


struct TopologyState {
    topology: Topology,
    pair_connection_id_map: HashMap<(NodeId, NodeId), LinkId>,
}

impl TopologyState {
    fn new() -> Self {
        TopologyState {
            topology: Topology::new(),
            pair_connection_id_map: HashMap::new(),
        }
    }

    fn get_switch_node_id(&self, name: &str) -> Result<NodeId, TopologyUpdateError> {
        match self.topology.get_switch_node_id(name) {
            Ok(id) => Ok(id),
            Err(_) => Err(TopologyUpdateError::SwitchDoesntExist(name.to_string())),
        }
    }

    fn add_switch(&mut self, name: &str) -> Result<(), TopologyUpdateError> {
        let switch_id = self.topology.get_next_switch_id();
        self.topology.add_switch(Switch::new(switch_id, name.to_string(), None))?;
        Ok(())
    }

    fn add_host(&mut self, name: &str, switch_name: &str) -> Result<(), TopologyUpdateError> {
        let switch_id = self.get_switch_node_id(switch_name)?;
        let host_id = self.topology.get_next_host_id();
        self.topology.add_host(Host::new(host_id, name.to_string(), switch_id))?;
        Ok(())
    }

    fn add_bidir_connection(&mut self,
                            switch_name1: &str,
                            switch_name2: &str,
                            weight: LinkWeight)
                            -> Result<(), TopologyUpdateError> {
        let switch1 = self.get_switch_node_id(switch_name1)?;
        let switch2 = self.get_switch_node_id(switch_name2)?;
        let link_id = self.topology.get_connection_n();
        self.topology.add_bidir_connection(Connection::new(switch1, switch2, weight));
        self.pair_connection_id_map.insert((switch1, switch2), link_id as LinkId);
        self.pair_connection_id_map.insert((switch2, switch1), link_id as LinkId);
        Ok(())
    }

    fn remove_bidir_connection(&mut self,
                               switch_name1: &str,
                               switch_name2: &str)
                               -> Result<(), TopologyUpdateError> {
        let switch1 = self.get_switch_node_id(switch_name1)?;
        let switch2 = self.get_switch_node_id(switch_name2)?;
        let link_id = match self.pair_connection_id_map.get(&(switch1, switch2)) {
                Some(id) => id,
                None => {
                    return Err(TopologyUpdateError::ConnectionDoesntExist(switch_name1.to_string(),
                                                                          switch_name2.to_string()))
                }
            }
            .clone();
        self.pair_connection_id_map.remove(&(switch1, switch2));
        self.pair_connection_id_map.remove(&(switch2, switch1));
        self.topology.remove_connection(link_id);
        Ok(())
    }

    fn get_connection(&self,
                      switch_id1: NodeId,
                      switch_id2: NodeId)
                      -> Result<&Connection, TopologyUpdateError> {
        let link_id = match self.pair_connection_id_map.get(&(switch_id1, switch_id2)) {
                Some(id) => id,
                None => {
                    return Err(TopologyUpdateError::ConnectionDoesntExist(switch_id1.to_string(),
                                                                          switch_id2.to_string()))
                }
            }
            .clone();
        Ok(self.topology.get_connection(link_id))
    }
}

#[derive(Debug)]
enum TopologyUpdateError {
    SwitchDoesntExist(String),
    ConnectionDoesntExist(String, String),
    Underlying(String),
}

impl fmt::Display for TopologyUpdateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TopologyUpdateError::SwitchDoesntExist(ref node) => {
                write!(f, "Switch '{}' doesn't exist", node)
            }
            TopologyUpdateError::ConnectionDoesntExist(ref node1, ref node2) => {
                write!(f,
                       "Connection between '{}' and '{}' doesn't exist",
                       node1,
                       node2)
            }
            TopologyUpdateError::Underlying(ref err) => {
                write!(f, "Topology returned error: '{}'", err)
            }
        }
    }
}

impl error::Error for TopologyUpdateError {
    fn description(&self) -> &str {
        "Error occured while updating topology"
    }
}

impl From<String> for TopologyUpdateError {
    fn from(err: String) -> TopologyUpdateError {
        TopologyUpdateError::Underlying(err)
    }
}

fn main() {
    drop(env_logger::init());
    timely_query::execute(|root, coord| {
        let (mut input, cap) = root.dataflow::<i32, _, _>(|scope| {
            let (input_tuple, network_updates) = scope.new_unordered_input::<String>();
            let mut connector = Connector::<String, String, _, _>::new(None, scope).unwrap();
            connector.register_with_coordinator("TopologyKeeper", &coord).unwrap();
            let clients_stream =
                connector.incoming_stream().inspect(|x| println!("From client: {:?}", x));

            let topology = TopologyState::new();

            // Ignoring incorrectly formatted updates.
            let network_updates = network_updates.flat_map(|x| {
                let parsed = parse_update(&x);
                match parsed {
                    Some(typ) => vec![typ],
                    None => {
                        warn!("This update is incorrectly formated: {}", x);
                        vec![]
                    }
                }
            });

            let state_operator =
                StateOperatorBuilder::new(topology, &network_updates, &clients_stream)
                    .state_unary_stream(Pipeline, "TopologyState", |state, input, output| {
                        input.for_each(|cap, data| {
                            let mut state = state.borrow_mut();
                            for update in data.iter() {
                                output.session(&cap).give(update.clone());
                                // Ignoring incorrect updates.
                                match update {
                                    &UpdateType::AddHost(ref hname, ref sname) => {
                                        if let Err(err) = state.add_host(hname, sname) {
                                            warn!("Adding host update failed: {}", err);
                                        }
                                    }
                                    &UpdateType::AddSwitch(ref sname) => {
                                        if let Err(err) = state.add_switch(sname) {
                                            warn!("Adding switch update failed: {}", err);
                                        }
                                    }
                                    &UpdateType::RemoveConnection(ref sname1, ref sname2) => {
                                        if let Err(err) =
                                            state.remove_bidir_connection(sname1, sname2) {
                                            warn!("Removing connection update failed: {}", err);
                                        }
                                    }
                                    &UpdateType::AddBidirConnection(ref sname1,
                                                                    ref sname2,
                                                                    ref weight) => {
                                        if let Err(err) =
                                            state.add_bidir_connection(sname1,
                                                                       sname2,
                                                                       weight.clone()) {
                                            warn!("Adding connection update failed: {}", err);
                                        }
                                    }
                                }
                            }
                        });
                    })
                    .clients_unary_stream(Pipeline, "TopologyResponder", |state, input, output| {
                        input.for_each(|cap, data| {
                            let state = state.borrow();
                            let mut session = output.session(&cap);
                            for cq in data.iter() {
                                let mut response = cq.create_response();
                                // TODO: this match should dissapear once we get the logic for
                                // updates
                                let query = match cq.query() {
                                    &KeeperQuery::Query(ref q) => q.to_string(),
                                    _ => "".to_string(),
                                };
                                match parse_query(&query) {
                                    Some(QueryType::GetAllHosts) => {
                                        for h in state.topology.all_hosts() {
                                            let ref switch_name =
                                                state.topology.get_switch(h.switch_id).name;
                                            response.add_tuple(
                                                KeeperResponse::Response(format!("H;{};{}",
                                                                         h.name,
                                                                         switch_name)));
                                        }
                                    }
                                    Some(QueryType::GetAllSwitches) => {
                                        for s in state.topology.all_switches() {
                                            response.add_tuple(
                                                KeeperResponse::Response(format!("S;{}", s.name)));
                                        }
                                    }
                                    Some(QueryType::GetAllConnections) => {
                                        for c in state.topology.all_connections() {
                                            let ref s1_name =
                                                state.topology.get_switch(c.from).name;
                                            let ref s2_name = state.topology.get_switch(c.to).name;
                                            response.add_tuple(
                                                KeeperResponse::Response(format!("C;{};{};{};{}",
                                                                        s1_name,
                                                                        s2_name,
                                                                        c.weight,
                                                                        1)));
                                        }
                                    }
                                    Some(QueryType::IsConnected(sname1, sname2)) => {
                                        let mut is_conn = 0;
                                        let mut weight = 0;
                                        loop {
                                            let s1_id = match state.get_switch_node_id(&sname1) {
                                                Ok(id) => id,
                                                Err(_) => break,
                                            };
                                            let s2_id = match state.get_switch_node_id(&sname2) {
                                                Ok(id) => id,
                                                Err(_) => break,
                                            };
                                            if state.topology.is_connected(s1_id, s2_id) {
                                                is_conn = 1;
                                            } else {
                                                break;
                                            }
                                            if let Ok(conn) = state.get_connection(s1_id, s2_id) {
                                                weight = conn.weight;
                                            }
                                            break;
                                        }
                                        response.add_tuple(
                                            KeeperResponse::Response(format!("C;{};{};{};{}",
                                                                    sname1,
                                                                    sname2,
                                                                    weight,
                                                                    is_conn)));
                                    }
                                    None => response.add_tuple(KeeperResponse::ConnectionEnd),
                                }

                                session.give(response);
                            }
                        });
                    })
                    .construct();
            let responses_stream = state_operator.get_outgoing_clients_stream();
            connector.outgoing_stream(responses_stream);
            input_tuple
        });
        let sub = coord.subscribe::<_, String>("topology_input".into(), cap).unwrap();
        for (time, data) in sub {
            input.session(time).give_content(&mut Content::Typed(data));
            root.step();
        }
    })
            .unwrap();
}

#[cfg(test)]
mod tests {
    use super::{UpdateType, parse_update, QueryType, parse_query};

    #[test]
    fn test_parse_update() {
        assert_eq!(Some(UpdateType::AddHost("host_1".to_string(), "switch_1".to_string())),
                   parse_update("AH;host_1;switch_1"));
        assert_eq!(None, parse_update("NOTHING;host_1;switch_1"));
        assert_eq!(Some(UpdateType::AddSwitch("s".to_string())),
                   parse_update("AS;s"));
        assert_eq!(None, parse_update("AS;switch;something_else"));
        assert_eq!(Some(UpdateType::RemoveConnection("switch1".to_string(),
                                                     "switch2".to_string())),
                   parse_update("RC;switch1;switch2"));
        assert_eq!(None, parse_update("RC;switch1;"));
        assert_eq!(None, parse_update("RC;switch1;"));
        assert_eq!(None, parse_update("RC;switch1;s2;s3"));
        assert_eq!(Some(UpdateType::AddBidirConnection("s1".to_string(), "s2".to_string(), 5)),
                   parse_update("AC;s1;s2;5"));
        assert_eq!(Some(UpdateType::AddBidirConnection("Switch_499".to_string(),
                                                       "Switch_499".to_string(),
                                                       5)),
                   parse_update("AC;Switch_499;Switch_499;5"));
        assert_eq!(None, parse_update("AC;s1;s2;s"));
        assert_eq!(None, parse_update("AC;s1;s2;s;4"));
    }

    #[test]
    fn test_parse_query() {
        assert_eq!(Some(QueryType::GetAllHosts), parse_query("GAH"));
        assert_eq!(None, parse_query("NOTHING"));
        assert_eq!(Some(QueryType::GetAllSwitches), parse_query("GAS"));
        assert_eq!(Some(QueryType::GetAllConnections), parse_query("GAC"));
        assert_eq!(Some(QueryType::IsConnected("n1".to_string(), "n2".to_string())),
                   parse_query("IC;n1;n2"));
        assert_eq!(None, parse_query("IC;d"));
        assert_eq!(None, parse_query("IC;d;d2;d4"));
    }
}
