use std::cmp;
use std::slice;

use timely::order::PartialOrder;
use timely::dataflow::operators::capture::Event;
use timely::progress::timestamp::Timestamp;
use timely::progress::frontier::{Antichain, MutableAntichain};

#[derive(Debug)]
pub struct ProgressState<T: Timestamp> {
    closed: MutableAntichain<T>, // the Timely frontier
    opened: Antichain<Rev<T>>,
}

#[derive(Debug, Serialize)]
pub struct Snapshot<'a, T: 'a> {
    /// A representation of all closed epochs.
    /// An epoch T is "closed" if it cannot be observed in the stream anymore. 
    /// This attribute is an antichain equvalent to the Timely frontier.
    pub closed: &'a [T],
    /// A representation of all open epochs.
    /// An epoch T is "open" if there has been a tuple on the stream with
    /// timestamp T'  where T' >= T. This attribute is an antichain.
    pub opened: &'a [T],
}

impl<T: Timestamp> ProgressState<T> {
    /// Creates a properly initialized progress state.
    ///
    /// The set of `upper` epochs of a new progress sate is empty, while the
    /// set of `lower` epochs is the singelton set containing `T::default()`.
    pub fn init() -> Self {
        ProgressState {
            closed: MutableAntichain::new_bottom(Default::default()),
            opened: Antichain::new(),
        }
    }

    /// Updates the frontier and returns the compacted list of changes to it
    pub fn update_frontier(&mut self, updates: Vec<(T, i64)>) -> Vec<(T, i64)> {
        let mut changes = Vec::new();
        self.closed.update_iter_and(updates, |time, diff| {
            changes.push((time.clone(), diff));
        });

        changes
    }

    /// Marks a timestamp as observed.
    /// 
    /// This can indicate the start of a new epoch, if the timestamp has not
    /// been observed before.
    pub fn insert_timestamp(&mut self, t: T) {
        self.opened.insert(Rev(t));
    }

    /// Returns a serializable snapshot of the current progress state
    pub fn snapshot<'a>(&'a self) -> Snapshot<'a, T> {
        // SAFETY: This assumes that Rev<T> and T use the same memory layout
        debug_assert_eq!(::std::mem::size_of::<Rev<T>>(), ::std::mem::size_of::<T>());
        debug_assert_eq!(::std::mem::align_of::<Rev<T>>(), ::std::mem::align_of::<T>());

        let opened_ptr = self.opened.elements().as_ptr() as *const T;
        let opened_len = self.opened.elements().len();
        Snapshot {
            closed: self.closed.frontier(),
            opened: unsafe { slice::from_raw_parts(opened_ptr, opened_len) },
        }
    }
}

/// A wrapper type implementing the *reverse* partial order of `T`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
// TODO(swicki): use #[repr(transparent)] once it's stable
struct Rev<T>(T);

impl<T: PartialOrder> PartialOrder for Rev<T> {
    fn less_equal(&self, other: &Self) -> bool {
        other.0.less_equal(&self.0)
    }
    
    fn less_than(&self, other: &Self) -> bool {
        other.0.less_than(&self.0)
    }
}

impl<T: PartialOrd> PartialOrd for Rev<T> {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl<T: Ord> Ord for Rev<T> {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::ProgressState;

    #[test]
    fn reversed_antichain() {
        let mut state = ProgressState::init();
        state.insert_timestamp(0);
        assert_eq!(state.snapshot().opened, &[0]);
        state.insert_timestamp(1);
        assert_eq!(state.snapshot().opened, &[1]);
        state.insert_timestamp(0);
        assert_eq!(state.snapshot().opened, &[1]);
        state.insert_timestamp(6);
        assert_eq!(state.snapshot().opened, &[6]);
    }

    #[test]
    fn maintain_frontier() {
        let mut state = ProgressState::<i32>::init();
        assert_eq!(state.snapshot().closed, &[Default::default()]);
        let updates = state.update_frontier(vec![(0, 1)]);
        assert_eq!(state.snapshot().closed, &[0]);
        assert_eq!(&updates, &[]);
        let updates = state.update_frontier(vec![(1, 1), (0, -2)]);
        assert_eq!(state.snapshot().closed, &[1]);
        assert_eq!(&updates, &[(0, -1), (1, 1)]);
    }
}
