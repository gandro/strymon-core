use std::any::Any;
use std::intrinsics::type_name;

use abomonation::Abomonation;

use super::*;

impl From<u64> for TopicId {
    fn from(id: u64) -> TopicId {
        TopicId(id)
    }
}

unsafe_abomonate!(TopicId);
unsafe_abomonate!(TopicType: name);
unsafe_abomonate!(Topic: id, name, addr, kind);

impl TopicType {
    pub fn of<T: Any>() -> Self {
        TopicType {
            id: TypeId::of::<T>(),
            name: unsafe { type_name::<T>() }.to_string(),
        }
    }
}

unsafe_abomonate!(QueryId);
unsafe_abomonate!(Query: id, name, program, workers, executors);
unsafe_abomonate!(QueryProgram: format, source, args);

unsafe_abomonate!(ExecutorId);
unsafe_abomonate!(Executor: id, host, format);
unsafe_abomonate!(ExecutionFormat);
