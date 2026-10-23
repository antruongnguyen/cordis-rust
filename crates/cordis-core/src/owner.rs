use crate::{
    AdmissionError, FiberId,
    effect::{Cleanup, Receiver},
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    rc::{Rc, Weak},
};

tokio::task_local! { static CALLBACKS: Vec<FiberId>; }

pub(crate) struct Owner {
    pub(crate) id: FiberId,
    pub(crate) name: String,
    pub(crate) live: Cell<bool>,
    pub(crate) cleanups: RefCell<BTreeMap<u64, Cleanup>>,
    pub(crate) draining: RefCell<Option<Receiver>>,
    parent: Weak<Owner>,
}
impl Owner {
    pub(crate) fn new(name: String) -> Rc<Self> {
        Rc::new(Self {
            id: FiberId::new(),
            name,
            live: Cell::new(true),
            cleanups: RefCell::new(BTreeMap::new()),
            draining: RefCell::new(None),
            parent: Weak::new(),
        })
    }
    pub(crate) fn child(name: String, parent: &Rc<Self>) -> Rc<Self> {
        Rc::new(Self {
            id: FiberId::new(),
            name,
            live: Cell::new(true),
            cleanups: RefCell::new(BTreeMap::new()),
            draining: RefCell::new(None),
            parent: Rc::downgrade(parent),
        })
    }
    pub(crate) fn admit(&self) -> Result<(), AdmissionError> {
        if self.live.get() {
            Ok(())
        } else {
            Err(AdmissionError::InactiveContext)
        }
    }
    fn ancestry(&self) -> Vec<FiberId> {
        let mut ids = CALLBACKS.try_with(Clone::clone).unwrap_or_default();
        ids.push(self.id.clone());
        let mut parent = self.parent.upgrade();
        while let Some(owner) = parent {
            ids.push(owner.id.clone());
            parent = owner.parent.upgrade();
        }
        ids
    }
    pub(crate) fn setup_scope<T>(&self, callback: impl FnOnce() -> T) -> T {
        CALLBACKS.sync_scope(self.ancestry(), callback)
    }
    pub(crate) async fn cleanup_scope<T>(&self, future: impl Future<Output = T>) -> T {
        CALLBACKS.scope(self.ancestry(), future).await
    }
    pub(crate) fn assert_not_cleaning(&self) -> Result<(), AdmissionError> {
        if CALLBACKS
            .try_with(|ids| ids.contains(&self.id))
            .unwrap_or(false)
        {
            Err(AdmissionError::ReentrantDrain)
        } else {
            Ok(())
        }
    }
}
