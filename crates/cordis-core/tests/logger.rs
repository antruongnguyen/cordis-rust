//! Minimal core diagnostics; full traced naming/formatting remains M6.
use cordis_core::{ExporterOptions, Level, LogError, Runtime};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    rc::Rc,
};

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
