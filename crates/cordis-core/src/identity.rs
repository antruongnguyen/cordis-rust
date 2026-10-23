use std::{
    fmt,
    hash::{Hash, Hasher},
    rc::Rc,
};

#[derive(Clone)]
struct Identity(Rc<()>);

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Identity {}

impl Hash for Identity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(Rc::as_ptr(&self.0), state);
    }
}

/// An opaque runtime identity. Equality is meaningful; its representation is not.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RuntimeId(Identity);

impl RuntimeId {
    pub(crate) fn new() -> Self {
        Self(Identity(Rc::new(())))
    }
}

impl fmt::Debug for RuntimeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeId(..)")
    }
}

/// An opaque Fiber identity, stable across restart and disposal. Each plugin
/// instance and the permanent root Fiber have distinct allocation identities.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FiberId(Identity);

impl FiberId {
    pub(crate) fn new() -> Self {
        Self(Identity(Rc::new(())))
    }
}

impl fmt::Debug for FiberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FiberId(..)")
    }
}

/// One service publication occurrence, independent of its name or value.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PublicationId(Identity);

impl PublicationId {
    pub(crate) fn new() -> Self {
        Self(Identity(Rc::new(())))
    }
}

impl fmt::Debug for PublicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PublicationId(..)")
    }
}

/// A shareable isolation slot. Equal descriptions are not equal identities.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct IsolationId(Identity);
impl IsolationId {
    /// Allocate an independent service realm.
    pub fn new() -> Self {
        Self(Identity(Rc::new(())))
    }
}
impl Default for IsolationId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Debug for IsolationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IsolationId(..)")
    }
}
