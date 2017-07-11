#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
// We need those in shared place compiled with both client and keeper. Maybe it will change when
// another RPC system is used.

// Prototype Keeper and Client
// ----------------------------------------------------
#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum PrototypeQueryType {
    ValueFor(String),
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
pub enum PrototypeKeyValueUpdate<V: abomonation::Abomonation> {
    Existing { key: String, value: V },
    Removed { key: String },
}
// ----------------------------------------------------
