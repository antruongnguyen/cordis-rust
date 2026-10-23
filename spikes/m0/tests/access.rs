use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};

#[derive(Clone)]
struct Label(Rc<()>);

impl Label {
    fn fresh() -> Self {
        Self(Rc::new(()))
    }
    fn id(&self) -> usize {
        Rc::as_ptr(&self.0) as usize
    }
}

struct Key<T> {
    name: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T> Key<T> {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }
}

type Config = BTreeMap<&'static str, i32>;

#[derive(Clone)]
struct Entry {
    name: &'static str,
    label: Label,
    value: Rc<dyn Any>,
    definition: Weak<Frame>,
    definition_layer: Rc<Layer>,
}

struct Declaration {
    committed: Option<Entry>,
    config: Config,
}

#[derive(Default)]
struct Catalog {
    names: BTreeMap<&'static str, (TypeId, Label)>,
    slots: BTreeMap<usize, Entry>,
}

struct Layer {
    parent: Option<Rc<Layer>>,
    boundary: Option<(&'static str, Label)>,
}

struct Frame {
    id: u32,
    parent: Option<View>,
    layer: Rc<Layer>,
    inject: BTreeMap<&'static str, Declaration>,
    own: RefCell<Vec<Entry>>,
    effects: RefCell<Vec<&'static str>>,
}

#[derive(Clone)]
struct View {
    catalog: Rc<RefCell<Catalog>>,
    frame: Rc<Frame>,
    layer: Rc<Layer>,
    intercepts: Vec<(&'static str, Config)>,
    metadata: BTreeMap<TypeId, Rc<dyn Any>>,
}

#[derive(Debug, PartialEq, Eq)]
enum AccessError {
    Undeclared,
    Inactive,
    Missing,
    TypeMismatch,
    Duplicate,
    DefinitionGone,
}

struct Bound<T> {
    value: Rc<T>,
    definition: View,
    usage: View,
    injection: Config,
    name: &'static str,
}

struct Call {
    definition: View,
    usage: View,
}

impl View {
    fn root() -> Self {
        let layer = Rc::new(Layer {
            parent: None,
            boundary: None,
        });
        Self {
            catalog: Rc::new(RefCell::new(Catalog::default())),
            frame: Rc::new(Frame {
                id: 0,
                parent: None,
                layer: layer.clone(),
                inject: BTreeMap::new(),
                own: RefCell::new(Vec::new()),
                effects: RefCell::new(Vec::new()),
            }),
            layer,
            intercepts: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn extend<T: 'static>(&self, value: T) -> Self {
        let mut child = self.clone();
        child.metadata.insert(TypeId::of::<T>(), Rc::new(value));
        child
    }

    fn metadata<T: 'static>(&self) -> Option<Rc<T>> {
        let value = self.metadata.get(&TypeId::of::<T>()).cloned()?;
        Some(
            value
                .downcast::<T>()
                .expect("metadata TypeId matches stored value"),
        )
    }

    fn label<T: 'static>(&self, key: &Key<T>) -> Result<Label, AccessError> {
        let mut catalog = self.catalog.borrow_mut();
        let (contract, default) = catalog
            .names
            .entry(key.name)
            .or_insert_with(|| (TypeId::of::<T>(), Label::fresh()));
        if *contract != TypeId::of::<T>() {
            return Err(AccessError::TypeMismatch);
        }
        let default = default.clone();
        drop(catalog);
        let mut cursor = Some(self.layer.clone());
        while let Some(layer) = cursor {
            if let Some((name, label)) = &layer.boundary {
                if *name == key.name {
                    return Ok(label.clone());
                }
            }
            cursor = layer.parent.clone();
        }
        Ok(default)
    }

    fn isolate<T>(&self, key: &Key<T>, label: Label) -> Self {
        Self {
            layer: Rc::new(Layer {
                parent: Some(self.layer.clone()),
                boundary: Some((key.name, label)),
            }),
            ..self.clone()
        }
    }

    fn intercept<T>(&self, key: &Key<T>, config: Config) -> Self {
        let mut child = self.clone();
        child.intercepts.push((key.name, config));
        child
    }

    fn child(&self, id: u32, inject: BTreeMap<&'static str, Declaration>) -> Self {
        Self {
            frame: Rc::new(Frame {
                id,
                parent: Some(self.clone()),
                layer: self.layer.clone(),
                inject,
                own: RefCell::new(Vec::new()),
                effects: RefCell::new(Vec::new()),
            }),
            ..self.clone()
        }
    }

    fn declaration<T: 'static>(&self, key: &Key<T>, config: Config) -> Declaration {
        let label = self.label(key).unwrap();
        Declaration {
            committed: self.catalog.borrow().slots.get(&label.id()).cloned(),
            config,
        }
    }

    fn provide<T: 'static>(&self, key: &Key<T>, value: T) -> Result<(), AccessError> {
        let label = self.label(key)?;
        if self.catalog.borrow().slots.contains_key(&label.id()) {
            return Err(AccessError::Duplicate);
        }
        let entry = Entry {
            name: key.name,
            label: label.clone(),
            value: Rc::new(value),
            definition: Rc::downgrade(&self.frame),
            definition_layer: self.layer.clone(),
        };
        self.catalog
            .borrow_mut()
            .slots
            .insert(label.id(), entry.clone());
        self.frame.own.borrow_mut().push(entry);
        Ok(())
    }

    fn locate<T: 'static>(&self, key: &Key<T>) -> Result<(Entry, Config), AccessError> {
        let label = self.label(key)?;
        if self.frame.parent.is_none() {
            return self
                .catalog
                .borrow()
                .slots
                .get(&label.id())
                .cloned()
                .map(|entry| (entry, Config::new()))
                .ok_or(AccessError::Missing);
        }
        let mut frame = self.frame.clone();
        let mut layer = self.layer.clone();
        loop {
            let own = frame
                .own
                .borrow()
                .iter()
                .find(|entry| entry.name == key.name && entry.label.id() == label.id())
                .cloned();
            if let Some(entry) = own {
                return Ok((entry, Config::new()));
            }
            while !Rc::ptr_eq(&layer, &frame.layer) {
                if layer
                    .boundary
                    .as_ref()
                    .is_some_and(|(name, _)| *name == key.name)
                {
                    return Err(AccessError::Undeclared);
                }
                layer = layer.parent.clone().ok_or(AccessError::Undeclared)?;
            }
            if let Some(declaration) = frame.inject.get(key.name) {
                return declaration
                    .committed
                    .clone()
                    .map(|entry| (entry, declaration.config.clone()))
                    .ok_or(AccessError::Inactive);
            }
            frame = frame
                .parent
                .as_ref()
                .map(|view| view.frame.clone())
                .ok_or(AccessError::Undeclared)?;
        }
    }

    fn checked<T: 'static>(&self, key: &Key<T>) -> Result<Bound<T>, AccessError> {
        let (entry, injection) = self.locate(key)?;
        let frame = entry
            .definition
            .upgrade()
            .ok_or(AccessError::DefinitionGone)?;
        let value = entry
            .value
            .downcast::<T>()
            .map_err(|_| AccessError::TypeMismatch)?;
        Ok(Bound {
            value,
            definition: Self {
                frame: frame.clone(),
                layer: entry.definition_layer,
                ..self.clone()
            },
            usage: self.clone(),
            injection,
            name: key.name,
        })
    }

    fn direct<T: 'static>(&self, key: &Key<T>) -> Result<Rc<T>, AccessError> {
        let label = self.label(key)?;
        self.catalog
            .borrow()
            .slots
            .get(&label.id())
            .ok_or(AccessError::Missing)?
            .value
            .clone()
            .downcast::<T>()
            .map_err(|_| AccessError::TypeMismatch)
    }
}

impl<T> Bound<T> {
    fn config(&self) -> Config {
        let mut config = self.injection.clone();
        for (name, layer) in &self.usage.intercepts {
            if *name == self.name {
                config.extend(layer)
            }
        }
        config
    }

    fn call<R>(&self, method: impl FnOnce(&T, Call) -> R) -> R {
        method(
            &self.value,
            Call {
                definition: self.definition.clone(),
                usage: self.usage.clone(),
            },
        )
    }
}

impl Call {
    fn service<T: 'static>(&self, key: &Key<T>) -> Result<Bound<T>, AccessError> {
        let mut bound = self.definition.checked(key)?;
        bound.usage = self.usage.clone();
        Ok(bound)
    }

    fn effect(&self, label: &'static str) {
        self.usage.frame.effects.borrow_mut().push(label);
    }
}

const FOO: Key<u32> = Key::new("foo");

#[test]
fn nearest_inject_own_provisions_and_same_label_boundaries() {
    let root = View::root();
    root.provide(&FOO, 1).unwrap();
    let label = root.label(&FOO).unwrap();
    let outer = root.child(
        1,
        BTreeMap::from([("foo", root.declaration(&FOO, Config::from([("a", 1)])))]),
    );
    let child = outer.child(2, BTreeMap::new());
    assert_eq!(*child.checked(&FOO).unwrap().value, 1);
    let isolated = outer.isolate(&FOO, label).child(3, BTreeMap::new());
    assert!(matches!(
        isolated.checked(&FOO),
        Err(AccessError::Undeclared)
    ));
    assert_eq!(*isolated.direct(&FOO).unwrap(), 1);
    let nearest = outer.child(
        4,
        BTreeMap::from([("foo", outer.declaration(&FOO, Config::from([("b", 2)])))]),
    );
    assert_eq!(
        nearest.checked(&FOO).unwrap().config(),
        Config::from([("b", 2)])
    );
    let own = outer.isolate(&FOO, Label::fresh());
    own.provide(&FOO, 2).unwrap();
    assert_eq!(*own.checked(&FOO).unwrap().value, 2);
    assert!(own.checked(&FOO).unwrap().config().is_empty());
    own.checked(&FOO).unwrap().call(|value, call| {
        assert_eq!(*call.service(&FOO).unwrap().value, *value);
    });
    assert_eq!(own.provide(&FOO, 3), Err(AccessError::Duplicate));
    let pending = root.child(
        5,
        BTreeMap::from([(
            "missing",
            Declaration {
                committed: None,
                config: Config::new(),
            },
        )]),
    );
    assert!(matches!(
        pending.checked(&Key::<u32>::new("missing")),
        Err(AccessError::Inactive)
    ));
}

#[test]
fn nested_calls_keep_definition_authority_and_use_site_intercepts_and_effects() {
    let root = View::root();
    let inner = root.child(1, BTreeMap::new());
    inner.provide(&FOO, 7).unwrap();
    let relay_key = Key::<()>::new("relay");
    let relay = root.child(
        2,
        BTreeMap::from([(
            "foo",
            root.declaration(&FOO, Config::from([("a", 1), ("b", 1)])),
        )]),
    );
    relay.provide(&relay_key, ()).unwrap();
    let use_site = root.intercept(&FOO, Config::from([("b", 2)])).child(
        3,
        BTreeMap::from([("relay", root.declaration(&relay_key, Config::new()))]),
    );
    let result = use_site
        .checked(&relay_key)
        .unwrap()
        .call(|(), relay_call| {
            let bound = relay_call.service(&FOO).unwrap();
            let config = bound.config();
            bound.call(|value, inner_call| {
                assert_eq!(inner_call.definition.frame.id, 1);
                assert_eq!(inner_call.usage.frame.id, 3);
                inner_call.effect("nested");
                (*value, config)
            })
        });
    assert_eq!(result, (7, Config::from([("a", 1), ("b", 2)])));
    assert_eq!(*use_site.frame.effects.borrow(), ["nested"]);
    assert!(inner.frame.effects.borrow().is_empty());
    assert!(relay.frame.effects.borrow().is_empty());

    let forbidden = root.child(4, BTreeMap::new());
    let forbidden_key = Key::<()>::new("forbidden");
    forbidden.provide(&forbidden_key, ()).unwrap();
    root.checked(&forbidden_key).unwrap().call(|(), call| {
        assert!(matches!(call.service(&FOO), Err(AccessError::Undeclared)));
    });
}

#[test]
fn typed_extension_and_explicit_accessor_keep_inherited_metadata() {
    let root = View::root().extend(String::from("outer"));
    root.provide(&FOO, 3).unwrap();
    let child = root.child(
        1,
        BTreeMap::from([("foo", root.declaration(&FOO, Config::new()))]),
    );
    let shadow = child.extend(String::from("inner"));
    assert_eq!(&*child.metadata::<String>().unwrap(), "outer");
    assert_eq!(&*shadow.metadata::<String>().unwrap(), "inner");
    assert_eq!(&*root.metadata::<String>().unwrap(), "outer");
    assert!(child.metadata::<bool>().is_none());
    let accessor = |context: &View| {
        context.checked(&FOO).map(|service| {
            service.call(|value, call| {
                call.effect("explicit accessor");
                value * 2
            })
        })
    };
    assert_eq!(accessor(&shadow), Ok(6));
    assert_eq!(*shadow.frame.effects.borrow(), ["explicit accessor"]);
    assert!(root.frame.effects.borrow().is_empty());
}

#[test]
fn named_type_contracts_shared_labels_and_plugin_identity_are_distinct() {
    let root = View::root();
    root.provide(&FOO, 1).unwrap();
    assert_eq!(
        root.direct(&Key::<String>::new("foo")),
        Err(AccessError::TypeMismatch)
    );
    let alias = Key::<u32>::new("other");
    root.provide(&alias, 2).unwrap();
    assert_eq!(*root.direct(&FOO).unwrap(), 1);
    assert_eq!(*root.direct(&alias).unwrap(), 2);
    let shared = Label::fresh();
    let a = root.isolate(&FOO, shared.clone());
    let b = root.isolate(&FOO, shared.clone());
    a.provide(&FOO, 3).unwrap();
    assert_eq!(*b.direct(&FOO).unwrap(), 3);
    // TS stores by isolation symbol, so a cross-name label aliases that slot too.
    let cross_name = root.isolate(&alias, shared);
    assert_eq!(*cross_name.direct(&alias).unwrap(), 3);
    assert_eq!(cross_name.provide(&alias, 4), Err(AccessError::Duplicate));
    let wrong = root.isolate(&Key::<String>::new("text"), a.label(&FOO).unwrap());
    assert_eq!(
        wrong.direct(&Key::<String>::new("text")),
        Err(AccessError::TypeMismatch)
    );

    struct PluginId(Rc<()>);
    let first = PluginId(Rc::new(()));
    let same = PluginId(first.0.clone());
    let other = PluginId(Rc::new(()));
    assert!(Rc::ptr_eq(&first.0, &same.0));
    assert!(!Rc::ptr_eq(&first.0, &other.0));
}
