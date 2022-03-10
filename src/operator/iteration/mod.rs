use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Condvar, Mutex};

// mod iterate;
// mod iteration_end;
// mod leader;
// mod replay;
// mod state_handler;

/// The information about the new state of an iteration:
///
/// - a boolean indicating if a new iteration should start
/// - the new state for the next iteration
pub(crate) type NewIterationState<State> = (bool, State);

/// A shared reference to the state of an iteration,
///
/// This state is shared between all the replicas inside the host. Additional synchronization must
/// be put in place for using this reference safely.
#[derive(Clone, Debug)]
pub(crate) struct IterationStateRef<State> {
    /// The storage for the state.
    ///
    /// Access it via `set` and `get` **with additional synchronization** in place.
    state: Arc<UnsafeCell<State>>,
}

impl<State> IterationStateRef<State> {
    fn new(init: State) -> Self {
        Self {
            state: Arc::new(UnsafeCell::new(init)),
        }
    }

    /// Change the value of the state with the one specified.
    ///
    /// ## Safety
    ///
    /// This will just write to a mutable pointer. Additional synchronization should be put in place
    /// before calling this method. All the references obtained with `get` should be dropped before
    /// calling this method, and no 2 thread can call this simultaneously.
    unsafe fn set(&self, new_state: State) {
        let state_ptr = &mut *self.state.get();
        *state_ptr = new_state;
    }

    /// Obtain a reference to the state.
    ///
    /// ## Safety
    ///
    /// This will just unsafely borrow the local state. Additional synchronization should be put in
    /// place before calling this method. The reference returned by this method should not be used
    /// while calling `set`.
    unsafe fn get(&self) -> &State {
        &*self.state.get()
    }
}

/// We grant that the user of this structs has put enough synchronization to avoid undefined
/// behaviour.
unsafe impl<State: Send + Sync> Send for IterationStateRef<State> {}

/// Handle to the state of the iteration.
#[derive(Clone)]
pub struct IterationStateHandle<T> {
    /// A reference to the output state that will be accessible by the user.
    result: IterationStateRef<T>,
}

impl<T: Clone> IterationStateHandle<T> {
    pub(crate) fn new(init: T) -> Self {
        Self {
            result: IterationStateRef::new(init),
        }
    }

    /// Set the new value for the state of the iteration.
    ///
    /// ## Safety
    ///
    /// It's important that the operator that manages this state takes extra care ensuring that no
    /// undefined behaviour due to data-race is present. Calling `set` while there are still some
    /// references from `get` around is forbidden. Calling `set` while another thread is calling
    /// `set` is forbidden.
    pub(crate) unsafe fn set(&self, new_state: T) {
        self.result.set(new_state);
    }

    /// Obtain a reference to the global iteration state.
    ///
    /// ## Safety
    ///
    /// The returned reference must not be stored in any way, especially between iteration
    /// boundaries (when the state is updated).
    pub fn get(&self) -> &T {
        unsafe { self.result.get() }
    }
}

impl<T> Debug for IterationStateHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterationStateHandle").finish()
    }
}

/// When the iteration block sends the `FlushAndRestart` message, the state of this host is in a
/// critical state: the iteration block does not update it until it receives the new state from the
/// leader and the downstream operators may access the state of the current iteration.
///
/// When the leader sends the new state we know for sure that all the downstream operators have
/// ended their computation and won't access the state until they receive the first message of the
/// next iteration. When there are shuffles this may lead to problems since the first message a
/// block receives can be from another host that received the state before this host. When this
/// happens the downstream operator will access the old state and this cause undefined behaviour
/// and data-race with the local leader.
///
/// To avoid this race we put in place a lock between iterations: when a block receives a
/// `FlushAndRestart`, before accepting any other message it waits for the local leader to update
/// the local state.
///
/// We cannot simply use a `Mutex<bool>` since it's possible that the state is unlocked, the stream
/// is consumed very quickly and the state is locked immediately after. If this happens fast enough
/// the downstream operators may fail to notice that the state got unlocked, and deadlock the
/// stream.
///
/// To avoid this problem, instead of keeping a _locked_ boolean, we keep the _generation_ of the
/// state: every time an iteration ends the generation is incremented. This allows the downstream
/// operators to know if they _skipped_ an iteration.
///
/// The value of the generation has this meaning:
/// - even value: the state is clean and can be accessed safely
/// - odd value: the state is locked, it cannot be locked again. The state can be accessed safely
///   with a generation lower or equal to this generation.
///
/// This means that locking and unlocking the state increments the generation by 2.
#[derive(Debug, Default)]
#[allow(clippy::mutex_atomic)]
pub(crate) struct IterationStateLock {
    /// The index of the generation.
    generation: Mutex<usize>,
    /// A conditional variable for notifying the downstream operators that the state got unlocked.
    cond_var: Condvar,
}

#[allow(clippy::mutex_atomic)]
impl IterationStateLock {
    /// Lock the state.
    ///
    /// This operation is idempotent until `unlock` is called.
    pub fn lock(&self) {
        let mut lock = self.generation.lock().unwrap();
        if *lock % 2 == 0 {
            *lock += 1;
        }
    }

    /// Unlock a locked state.
    ///
    /// This will notify all the operators that are waiting for the state to be updated.
    pub fn unlock(&self) {
        let mut lock = self.generation.lock().unwrap();
        assert_eq!(*lock % 2, 1, "cannot unlock a non-locked lock");
        *lock += 1;
        self.cond_var.notify_all();
    }

    /// Block the thread if the current generation of the lock is lower that the requested one.
    pub fn wait_for_update(&self, generation: usize) {
        let _gen = self
            .cond_var
            .wait_while(self.generation.lock().unwrap(), |r| *r < generation)
            .unwrap();
    }
}
