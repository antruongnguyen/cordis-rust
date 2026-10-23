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
    pub(crate) weak: Weak<Owner>,
    pub(crate) id: FiberId,
    pub(crate) name: String,
    pub(crate) live: Cell<bool>,
    pub(crate) cleanups: RefCell<BTreeMap<u64, Cleanup>>,
    pub(crate) draining: RefCell<Option<Receiver>>,
    pub(crate) parent: Weak<Owner>,
    pub(crate) fiber: RefCell<Weak<crate::plugin::Fiber>>,
    pub(crate) dependencies: Vec<crate::Dependency>,
    pub(crate) committed: RefCell<Option<crate::service::Bindings>>,
    pub(crate) provisions: RefCell<Vec<Rc<crate::service::Publication>>>,
    pub(crate) scope: Option<Rc<crate::context::IsolationLayer>>,
    pub(crate) services_paused: Cell<bool>,
    pub(crate) acquisitions: RefCell<Vec<Weak<Cell<bool>>>>,
    root: bool,
}
impl Owner {
    pub(crate) fn new(name: String) -> Rc<Self> {
        Rc::new_cyclic(|weak| Self {
            weak: weak.clone(),
            id: FiberId::new(),
            name,
            live: Cell::new(true),
            cleanups: RefCell::new(BTreeMap::new()),
            draining: RefCell::new(None),
            parent: Weak::new(),
            fiber: RefCell::new(Weak::new()),
            dependencies: Vec::new(),
            committed: RefCell::new(None),
            provisions: RefCell::new(Vec::new()),
            scope: None,
            services_paused: Cell::new(false),
            acquisitions: RefCell::new(Vec::new()),
            root: true,
        })
    }
    pub(crate) fn child(
        name: String,
        parent: &Rc<Self>,
        dependencies: Vec<crate::Dependency>,
        scope: Option<Rc<crate::context::IsolationLayer>>,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak| Self {
            weak: weak.clone(),
            id: FiberId::new(),
            name,
            live: Cell::new(true),
            cleanups: RefCell::new(BTreeMap::new()),
            draining: RefCell::new(None),
            parent: Rc::downgrade(parent),
            fiber: RefCell::new(Weak::new()),
            dependencies,
            committed: RefCell::new(None),
            provisions: RefCell::new(Vec::new()),
            scope,
            services_paused: Cell::new(false),
            acquisitions: RefCell::new(Vec::new()),
            root: false,
        })
    }
    pub(crate) fn is_root(&self) -> bool {
        self.root
    }
    pub(crate) fn services_active(&self) -> bool {
        !self.services_paused.get()
            && (self.root
                || self
                    .fiber
                    .borrow()
                    .upgrade()
                    .is_some_and(|fiber| fiber.state() == crate::FiberState::Active))
    }
    pub(crate) fn stop_acquisitions(&self) {
        let flags: Vec<_> = self
            .acquisitions
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for flag in flags {
            flag.set(true);
        }
    }
    pub(crate) fn commit(&self, bindings: crate::service::Bindings) {
        for publication in &bindings {
            publication.publication.retain(self);
        }
        *self.committed.borrow_mut() = Some(bindings);
    }
    pub(crate) fn release_dependencies(&self) {
        let bindings = self.committed.borrow_mut().take();
        for publication in bindings.into_iter().flatten() {
            publication.publication.release(self);
        }
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
