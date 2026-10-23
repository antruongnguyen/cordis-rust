#[derive(Clone, Debug, PartialEq, Eq)]
struct Source {
    count: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Resolved {
    count: u32,
    validated: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct Issue {
    path: Vec<&'static str>,
    message: &'static str,
}

fn validate(source: &Source) -> Result<Resolved, Issue> {
    let count = u32::try_from(source.count).map_err(|_| Issue {
        path: vec!["count"],
        message: "must be nonnegative",
    })?;
    Ok(Resolved {
        count,
        validated: true,
    })
}

struct ConfigFiber {
    loaded: bool,
    source: Source,
    applied: Vec<Resolved>,
    committed: Vec<Source>,
}

impl ConfigFiber {
    fn load(&mut self) -> Result<(), Issue> {
        let config = validate(&self.source)?;
        self.applied.push(config);
        self.loaded = true;
        Ok(())
    }

    fn update(
        &mut self,
        source: Source,
        no_save: bool,
        accepts: impl FnOnce(&Resolved) -> bool,
    ) -> Result<(), Issue> {
        let resolved = if self.loaded {
            Some(validate(&source)?)
        } else {
            None
        };
        if !no_save {
            self.committed.push(source.clone())
        }
        self.source = source;
        if let Some(resolved) = resolved {
            if !accepts(&resolved) {
                self.load()?
            }
        }
        Ok(())
    }
}

#[test]
fn invalid_loaded_update_cannot_reach_middleware_or_mutate_source() {
    let mut fiber = ConfigFiber {
        loaded: false,
        source: Source { count: 1 },
        applied: Vec::new(),
        committed: Vec::new(),
    };
    fiber.load().unwrap();
    assert_eq!(
        fiber.update(Source { count: -1 }, false, |_| panic!(
            "invalid config reached middleware"
        )),
        Err(Issue {
            path: vec!["count"],
            message: "must be nonnegative"
        })
    );
    assert_eq!(fiber.source, Source { count: 1 });
    assert_eq!(
        fiber.applied,
        [Resolved {
            count: 1,
            validated: true
        }]
    );
    assert!(fiber.committed.is_empty());
    fiber
        .update(Source { count: 2 }, false, |config| {
            assert!(config.validated);
            true
        })
        .unwrap();
    assert_eq!(fiber.source, Source { count: 2 });
    assert_eq!(fiber.committed, [Source { count: 2 }]);
    assert_eq!(fiber.applied.len(), 1);
    fiber.update(Source { count: 3 }, true, |_| false).unwrap();
    assert_eq!(fiber.source, Source { count: 3 });
    assert_eq!(fiber.committed.len(), 1);
    assert_eq!(fiber.applied.last().unwrap().count, 3);
}

#[test]
fn pending_defers_validation_and_loads_only_latest_source() {
    let mut fiber = ConfigFiber {
        loaded: false,
        source: Source { count: 0 },
        applied: Vec::new(),
        committed: Vec::new(),
    };
    fiber
        .update(Source { count: -1 }, false, |_| {
            panic!("pending middleware")
        })
        .unwrap();
    assert!(!fiber.loaded);
    assert!(fiber.load().is_err());
    fiber
        .update(Source { count: 2 }, false, |_| panic!("pending middleware"))
        .unwrap();
    fiber
        .update(Source { count: 3 }, false, |_| panic!("pending middleware"))
        .unwrap();
    fiber.load().unwrap();
    assert_eq!(
        fiber.applied,
        [Resolved {
            count: 3,
            validated: true
        }]
    );
}
