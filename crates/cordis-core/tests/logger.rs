//! Core diagnostics plus M6 traced naming, error identity, formatting, and palette evidence.
use cordis_core::{
    BoxError, ColorDepth, ErrorValue, ExporterOptions, Level, LogError, LoggerConfig, Plugin,
    Runtime, ServiceKey,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    rc::Rc,
};

#[tokio::test(flavor = "current_thread")]
async fn nested_service_names_restore_and_explicit_and_intercept_names_have_correct_precedence() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let logger = ServiceKey::<()>::new("logger");
            let inner = ServiceKey::<()>::new("inner");
            let outer = ServiceKey::<()>::new("outer");
            let records = Rc::new(RefCell::new(Vec::new()));
            root.add_exporter(
                ExporterOptions {
                    default_level: Some(Level::Debug),
                    ..Default::default()
                },
                {
                    let records = records.clone();
                    move |record| {
                        records.borrow_mut().push(record.clone());
                        Ok::<_, Infallible>(())
                    }
                },
            )
            .unwrap();
            let inner_fiber = root
                .plugin(
                    &Plugin::initialized("BarDriver", {
                        let inner = inner.clone();
                        move |ctx, ()| {
                            let inner = inner.clone();
                            async move {
                                ctx.provide(&inner, ())?;
                                ctx.logger(None).debug("initializing")?;
                                Ok::<_, BoxError>(cordis_core::Disposer::sync(|| {
                                    Ok::<_, Infallible>(())
                                }))
                            }
                        }
                    }),
                    (),
                )
                .unwrap();
            inner_fiber.ready().unwrap().await.unwrap();
            assert_eq!(records.borrow()[0].name, "bar-driver");
            let outer_fiber = root
                .plugin(
                    &Plugin::new("FooDriver", {
                        let outer = outer.clone();
                        move |ctx, ()| {
                            let outer = outer.clone();
                            async move {
                                ctx.provide(&outer, ())?;
                                Ok::<_, BoxError>(())
                            }
                        }
                    })
                    .injecting([inner.required()]),
                    (),
                )
                .unwrap();
            outer_fiber.ready().unwrap().await.unwrap();
            records.borrow_mut().clear();
            root.bind(&outer).unwrap().call(|_, ctx| {
                ctx.bind(&inner)
                    .unwrap()
                    .call(|_, nested| nested.logger(None).debug("inner").unwrap());
                ctx.logger(None).debug("outer").unwrap();
            });
            assert_eq!(
                records
                    .borrow()
                    .iter()
                    .map(|r| (r.name.as_str(), r.message.as_str()))
                    .collect::<Vec<_>>(),
                [("bar-driver", "inner"), ("foo-driver", "outer")]
            );
            assert_eq!(records.borrow()[0].fiber, *inner_fiber.id());
            assert_eq!(records.borrow()[1].fiber, *outer_fiber.id());
            let overridden = root.intercept(
                &logger,
                LoggerConfig {
                    name: Some("caller".into()),
                    level: Some(Level::Debug),
                },
            );
            overridden.bind(&outer).unwrap().call(|_, ctx| {
                ctx.bind(&inner)
                    .unwrap()
                    .call(|_, nested| nested.logger(None).debug("override").unwrap());
                assert_eq!(
                    ctx.logger(Some("explicit")).info("named").unwrap().name,
                    "explicit"
                );
            });
            assert_eq!(records.borrow()[2].name, "caller");
            let cached = overridden.logger(None);
            let changed = overridden.intercept(
                &logger,
                LoggerConfig {
                    name: Some("later".into()),
                    ..Default::default()
                },
            );
            assert_eq!(cached.info("cached").unwrap().name, "caller");
            assert_eq!(changed.logger(None).info("new").unwrap().name, "later");
            let count = root.log_buffer().unwrap().len();
            assert!(matches!(
                root.intercept(&logger, 3_u32).logger(None).info("bad"),
                Err(LogError::Configuration(_))
            ));
            assert_eq!(root.log_buffer().unwrap().len(), count);
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[test]
fn error_causes_and_aggregates_preserve_empty_aggregates_exact_sources_and_order() {
    let runtime = Runtime::new();
    let logger = runtime.context().logger(None);
    let inner = Rc::new(std::io::Error::other("inner"));
    let empty = logger
        .log_error(
            Level::Warn,
            ErrorValue::new(std::io::Error::other("empty")).with_members(vec![]),
        )
        .unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0].message, "empty");
    let flattened = logger
        .log_error(
            Level::Warn,
            ErrorValue::new(std::io::Error::other("outer"))
                .with_members(vec![ErrorValue::from_rc(inner.clone())]),
        )
        .unwrap();
    assert_eq!(flattened.len(), 1);
    assert_eq!(flattened[0].message, "inner");
    let exact = std::error::Error::source(flattened[0].failure.as_ref().unwrap())
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert!(std::ptr::eq(exact, Rc::as_ref(&inner)));
    let caused = logger
        .log_error(
            Level::Error,
            ErrorValue::new(std::io::Error::other("outer"))
                .with_members(vec![ErrorValue::new(std::io::Error::other(
                    "ignored member",
                ))])
                .with_cause(ErrorValue::from_rc(inner)),
        )
        .unwrap();
    assert_eq!(
        caused
            .iter()
            .map(|record| record.message.as_str())
            .collect::<Vec<_>>(),
        ["inner", "outer"]
    );
    assert!(caused[0].sequence < caused[1].sequence);
    assert_eq!(runtime.context().log_buffer().unwrap().len(), 4);
}

#[test]
fn formatting_obeys_per_line_utf16_budgets_crlf_and_exact_palette_shapes() {
    let runtime = Runtime::new();
    let logger = runtime.context().logger(None);
    let record = logger.info("x".repeat(10241)).unwrap();
    assert_eq!(record.format(10240), format!("{}...", "x".repeat(10240)));
    assert_eq!(logger.info("a\r\nb\r\n").unwrap().format(1), "a\nb\n");
    assert_eq!(logger.info("a\r").unwrap().format(3), "a\r");
    assert_eq!(logger.info("abc").unwrap().format(0), "...");
    assert_eq!(logger.info("").unwrap().format(0), "");
    let unicode = logger.info("x\u{1f600}y").unwrap();
    assert_eq!(unicode.format(2), "x...");
    assert_eq!(unicode.format(3), "x\u{1f600}...");
    assert_eq!(unicode.format(4), "x\u{1f600}y");
    assert_eq!(record.color("value", ColorDepth::None), "value");
    assert_eq!(
        record.color("value", ColorDepth::Ansi16),
        "\u{1b}[36mvalue\u{1b}[0m"
    );
    assert_eq!(
        record.color("value", ColorDepth::Ansi256),
        "\u{1b}[38;5;41mvalue\u{1b}[0m"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn normalized_diagnostic_names_keep_acronyms_delimiters_and_nonowning_attribution() {
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let fiber = root
                .plugin(
                    &Plugin::new("HTTPServer_ID", |_, ()| async {
                        Err::<(), _>(std::io::Error::other("failed"))
                    }),
                    (),
                )
                .unwrap();
            assert!(fiber.ready().unwrap().await.is_err());
            let buffer = root.log_buffer().unwrap();
            assert_eq!(buffer.last().unwrap().name, "http-server-id");
            assert_eq!(buffer.last().unwrap().fiber, *fiber.id());
            let logger = fiber.context().logger(None);
            fiber.dispose().unwrap().await.unwrap();
            let record = logger.info("after disposal").unwrap();
            assert_eq!(record.name, "http-server-id");
            assert_eq!(
                record.fiber_ref.snapshot().unwrap().state,
                cordis_core::FiberState::Disposed
            );
            drop(fiber);
            assert!(
                record.fiber_ref.snapshot().is_none(),
                "log metadata must not retain the disposed Fiber"
            );
            for (name, expected) in [
                ("_foo__Bar", "foo-bar"),
                ("foo2Bar", "foo2-bar"),
                ("2Foo", "2foo"),
                ("foo:Driver", "foo:-driver"),
            ] {
                let instance = root
                    .plugin(
                        &Plugin::new(name, |_, ()| async { Ok::<_, Infallible>(()) }),
                        (),
                    )
                    .unwrap();
                instance.ready().unwrap().await.unwrap();
                assert_eq!(instance.context().name(), name);
                assert_eq!(
                    instance
                        .context()
                        .logger(None)
                        .info("normalize")
                        .unwrap()
                        .name,
                    expected
                );
                instance.dispose().unwrap().await.unwrap();
            }
            root.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[test]
fn default_buffer_has_exact_capacity_chronological_order_and_root_attribution() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let logger = ctx.logger(None);
    for value in 0..1005 {
        logger.info(value).unwrap();
    }
    let buffer = ctx.log_buffer().unwrap();
    assert_eq!(buffer.len(), 1000);
    assert_eq!(buffer.first().unwrap().sequence, 6);
    assert_eq!(buffer.last().unwrap().sequence, 1005);
    assert_eq!(buffer.first().unwrap().message, "5");
    assert_eq!(buffer.last().unwrap().message, "1004");
    for record in &buffer {
        assert_eq!(record.name, "root");
        assert_eq!(record.fiber, *ctx.fiber_id());
        assert_eq!(record.level, Level::Info);
    }
    assert!(
        buffer
            .windows(2)
            .all(|records| records[0].sequence + 1 == records[1].sequence)
    );
    drop(runtime);
    assert!(matches!(
        logger.info("expired"),
        Err(LogError::RuntimeDropped)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn exporter_removal_is_exact_and_buffer_survives_reusable_root_drains() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let calls = Rc::new(Cell::new(0));
    runtime
        .run(async {
            let callback = Rc::new({
                let calls = calls.clone();
                move |_: &cordis_core::LogRecord| {
                    calls.set(calls.get() + 1);
                    Ok::<_, Infallible>(())
                }
            });
            let first = ctx
                .add_exporter(ExporterOptions::default(), {
                    let callback = callback.clone();
                    move |record| callback(record)
                })
                .unwrap();
            let _second = ctx
                .add_exporter(ExporterOptions::default(), move |record| callback(record))
                .unwrap();
            ctx.logger(Some("explicit")).info("one").unwrap();
            assert_eq!(calls.get(), 2);
            assert!(first.remove().unwrap());
            ctx.logger(None).warn("two").unwrap();
            assert_eq!(calls.get(), 3);
            ctx.drain().unwrap().await.unwrap();
            ctx.logger(None).error("three").unwrap();
            assert_eq!(calls.get(), 3);
            ctx.drain().unwrap().await.unwrap();
            ctx.logger(None).info("four").unwrap();
            let records = ctx.log_buffer().unwrap();
            assert_eq!(
                records
                    .iter()
                    .map(|r| r.message.as_str())
                    .collect::<Vec<_>>(),
                ["one", "two", "three", "four"]
            );
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn named_level_overrides_win_and_exporters_select_independently() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let selected = Rc::new(RefCell::new(Vec::new()));
    runtime
        .run(async {
            let options = ExporterOptions {
                default_level: Some(Level::Debug),
                levels: std::collections::BTreeMap::from([("quiet".into(), Level::Error)]),
            };
            let _export = ctx
                .add_exporter(options, {
                    let selected = selected.clone();
                    move |record| {
                        selected
                            .borrow_mut()
                            .push((record.name.clone(), record.level));
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap();
            ctx.logger(Some("verbose"))
                .debug("exporter-only debug")
                .unwrap();
            ctx.logger(Some("quiet")).info("not exported").unwrap();
            ctx.logger(Some("quiet")).error("exported").unwrap();
            assert_eq!(
                *selected.borrow(),
                [
                    ("verbose".into(), Level::Debug),
                    ("quiet".into(), Level::Error)
                ]
            );
            assert_eq!(
                ctx.log_buffer().unwrap().len(),
                2,
                "default buffer filters debug independently"
            );
            ctx.logger(None)
                .with_level(Level::Debug)
                .debug("buffer debug")
                .unwrap();
            assert_eq!(
                ctx.log_buffer().unwrap().last().unwrap().message,
                "buffer debug"
            );
            ctx.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn callback_diagnostics_use_logger_and_failing_exporters_cannot_skip_others() {
    let runtime = Runtime::new();
    let ctx = runtime.context();
    let delivered = Rc::new(Cell::new(0));
    runtime.run(async{
        let effect=ctx.on_dispose(||async{Err::<(),_>(std::io::Error::other("original cleanup"))}).unwrap();
        let _failed=ctx.add_exporter(ExporterOptions::default(),|_|Err::<(),_>(std::io::Error::other("export failed"))).unwrap();
        let _panic=ctx.add_exporter(ExporterOptions::default(),|_|->Result<(),Infallible>{panic!("export panic")}).unwrap();
        let _good=ctx.add_exporter(ExporterOptions::default(),{let delivered=delivered.clone();move|_|{delivered.set(delivered.get()+1);Ok::<_,Infallible>(())}}).unwrap();
        assert!(matches!(ctx.logger(None).info("user record"),Err(LogError::Exporters(errors))if errors.len()==2));assert_eq!(delivered.get(),1);
        let report=effect.dispose().unwrap().await.unwrap();assert_eq!(report.failures().len(),1);assert_eq!(delivered.get(),2);
        let failures=runtime.take_failures();assert_eq!(failures.len(),3,"original and both diagnostic exporter sources are retained without recursion");
        let records=ctx.log_buffer().unwrap();let diagnostic=records.last().unwrap();assert_eq!(diagnostic.name,"cordis");assert!(diagnostic.failure.is_some());
        ctx.drain().unwrap().await.unwrap();
    }).await.unwrap();
}
