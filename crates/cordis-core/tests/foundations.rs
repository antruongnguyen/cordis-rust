//! Integrated M1 ownership, hosting, and root-cleanup contracts.

use cordis_core::{AdmissionError, CompletionError, HostError, Runtime};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    error::Error,
    fmt,
    future::Future,
    rc::Rc,
};
use tokio::sync::oneshot;

async fn bounded(test: impl Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), test)
        .await
        .expect("foundation test stalled");
}

#[test]
fn independent_runtime_identities_and_layered_metadata() {
    let first = Runtime::new();
    let second = Runtime::new();
    let root = first.context();
    let parent = root.extend(String::from("parent"));
    let child = parent.extend(42_u32).extend(String::from("child"));
    assert_eq!(root.runtime_id(), child.runtime_id());
    assert_eq!(root.fiber_id(), child.fiber_id());
    assert_ne!(root.runtime_id(), second.context().runtime_id());
    assert_ne!(root.fiber_id(), second.context().fiber_id());
    assert_eq!(&*parent.metadata::<String>().unwrap(), "parent");
    assert_eq!(&*child.metadata::<String>().unwrap(), "child");
    assert_eq!(*child.metadata::<u32>().unwrap(), 42);
    assert!(root.metadata::<String>().is_none());
    assert!(child.metadata::<bool>().is_none());
    assert!(child.root().metadata::<String>().is_none());
    assert_eq!(child.root().fiber_id(), root.fiber_id());
    let ids =
        std::collections::HashSet::from([root.runtime_id().clone(), child.runtime_id().clone()]);
    assert_eq!(ids.len(), 1);
    assert_eq!(format!("{:?}", root.runtime_id()), "RuntimeId(..)");
}

#[test]
fn missing_executor_and_unhosted_registration_are_explicit_and_inert() {
    let runtime = Runtime::new();
    let context = runtime.context();
    let calls = Rc::new(Cell::new(0));
    let result = context.on_dispose({
        let calls = calls.clone();
        move || async move {
            calls.set(calls.get() + 1);
            Ok::<_, Infallible>(())
        }
    });
    assert_eq!(result.err(), Some(AdmissionError::HostNotRunning));
    assert_eq!(context.drain().err(), Some(AdmissionError::HostNotRunning));
    let run = runtime.run(async { panic!("future polled without executor") });
    tokio::pin!(run);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    assert_eq!(
        std::future::Future::poll(run.as_mut(), &mut cx),
        std::task::Poll::Ready(Err(HostError::MissingExecutor))
    );
    assert_eq!(calls.get(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_host_nested_host_and_raw_task_admission_are_refused() {
    let first = Runtime::new();
    let second = Runtime::new();
    first
        .run(async {
            assert_eq!(
                second.context().drain().err(),
                Some(AdmissionError::WrongHost)
            );
            assert_eq!(second.run(async {}).await, Err(HostError::NestedHost));
            assert_eq!(first.run(async {}).await, Err(HostError::NestedHost));
            let context = first.context();
            let outcome = tokio::task::spawn_local(async move { context.drain().err() })
                .await
                .unwrap();
            assert_eq!(outcome, Some(AdmissionError::HostNotRunning));
            let outcome = tokio::task::spawn_local(async {
                let host = Runtime::new();
                host.run(async {}).await
            })
            .await
            .unwrap();
            assert_eq!(outcome, Err(HostError::SpawnedTask));
            assert_eq!(
                first.context().drain().unwrap().await.unwrap().attempted(),
                0
            );
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_tokens_keeps_root_ownership_and_manual_claims_run_once() {
    let runtime = Runtime::new();
    let context = runtime.context();
    let calls = Rc::new(Cell::new(0));
    runtime
        .run(async {
            let token = context
                .on_dispose({
                    let calls = calls.clone();
                    move || async move {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap();
            let drain = context.drain().unwrap();
            assert_eq!(token.dispose().unwrap().await.unwrap().attempted(), 0);
            assert_eq!(drain.await.unwrap().attempted(), 1);
            assert_eq!(calls.get(), 1);
            let manual = context
                .on_dispose({
                    let calls = calls.clone();
                    move || async move {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap();
            assert_eq!(manual.dispose().unwrap().await.unwrap().attempted(), 1);
            let token = context
                .on_dispose({
                    let calls = calls.clone();
                    move || async move {
                        calls.set(calls.get() + 1);
                        Ok::<_, Infallible>(())
                    }
                })
                .unwrap();
            drop(token);
            assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 1);
            assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 0);
            assert_eq!(calls.get(), 3);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn eager_snapshot_drain_is_concurrent_shared_and_root_remains_usable() {
    bounded(async {
        let runtime = Runtime::new();
        let context = runtime.context();
        let trace = Rc::new(RefCell::new(Vec::new()));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        runtime
            .run(async {
                let _first = context
                    .on_dispose({
                        let trace = trace.clone();
                        move || async move {
                            trace.borrow_mut().push("first");
                            Ok::<_, Infallible>(())
                        }
                    })
                    .unwrap();
                let _second = context
                    .on_dispose({
                        let trace = trace.clone();
                        move || async move {
                            trace.borrow_mut().push("second start");
                            entered_tx.send(()).unwrap();
                            release_rx.await.unwrap();
                            trace.borrow_mut().push("second end");
                            Ok::<_, Infallible>(())
                        }
                    })
                    .unwrap();
                let first_waiter = context.drain().unwrap();
                let second_waiter = context.drain().unwrap();
                drop(first_waiter);
                let _late = context
                    .on_dispose({
                        let trace = trace.clone();
                        move || async move {
                            trace.borrow_mut().push("late");
                            Ok::<_, Infallible>(())
                        }
                    })
                    .unwrap();
                entered_rx.await.unwrap();
                assert_eq!(*trace.borrow(), ["second start", "first"]);
                release_tx.send(()).unwrap();
                assert_eq!(second_waiter.await.unwrap().attempted(), 2);
                assert_eq!(*trace.borrow(), ["second start", "first", "second end"]);
                assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 1);
                assert_eq!(
                    *trace.borrow(),
                    ["second start", "first", "second end", "late"]
                );
                assert!(runtime.take_failures().is_empty());
            })
            .await
            .unwrap();
    })
    .await;
}

#[derive(Debug)]
struct UserError(u32);
impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource {} failed", self.0)
    }
}
impl Error for UserError {}

#[tokio::test(flavor = "current_thread")]
async fn user_failure_sources_survive_and_panics_do_not_skip_other_cleanup() {
    let runtime = Runtime::new();
    let context = runtime.context();
    runtime
        .run(async {
            let _normal = context
                .on_dispose(|| async { Ok::<_, Infallible>(()) })
                .unwrap();
            let _failed = context
                .on_dispose(|| async { Err::<(), _>(UserError(7)) })
                .unwrap();
            let _call_panic = context
                .on_dispose(|| -> std::future::Ready<Result<(), Infallible>> {
                    panic!("call panic")
                })
                .unwrap();
            let _poll_panic = context
                .on_dispose(|| async {
                    panic!("poll panic");
                    #[allow(unreachable_code)]
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            let report = context.drain().unwrap().await.unwrap();
            assert_eq!(report.attempted(), 4);
            assert_eq!(report.failures().len(), 3);
            assert!(report.failures()[0].is_panic());
            assert!(report.failures()[1].is_panic());
            let returned = &report.failures()[2];
            assert!(!returned.is_panic());
            assert_eq!(
                returned
                    .source()
                    .unwrap()
                    .downcast_ref::<UserError>()
                    .unwrap()
                    .0,
                7
            );
            assert_eq!(runtime.take_failures().len(), 3);
            assert!(runtime.take_failures().is_empty());
            assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 0);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn captured_context_ancestry_and_reentrant_requests_do_not_create_cycles() {
    let runtime = Runtime::new();
    let root = runtime.context();
    let probe = Rc::new(());
    let weak = Rc::downgrade(&probe);
    let derived = root.extend(probe.clone()).extend(7_u32);
    drop(probe);
    runtime
        .run(async {
            let captured = derived.clone();
            let _effect = derived
                .on_dispose(move || async move {
                    assert!(captured.metadata::<Rc<()>>().is_some());
                    assert_eq!(captured.drain().err(), Some(AdmissionError::ReentrantDrain));
                    let _next = captured
                        .on_dispose(|| async { Ok::<_, Infallible>(()) })
                        .unwrap();
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            assert_eq!(root.drain().unwrap().await.unwrap().attempted(), 1);
            assert_eq!(root.drain().unwrap().await.unwrap().attempted(), 1);
        })
        .await
        .unwrap();
    drop(derived);
    assert!(weak.upgrade().is_none());

    let retained = Rc::new(());
    let weak = Rc::downgrade(&retained);
    let captured = root.extend(retained);
    runtime
        .run(async {
            let _effect = root
                .on_dispose(move || async move {
                    assert!(captured.metadata::<Rc<()>>().is_some());
                    Ok::<_, Infallible>(())
                })
                .unwrap();
        })
        .await
        .unwrap();
    assert!(weak.upgrade().is_some());
    drop(runtime);
    assert!(
        weak.upgrade().is_none(),
        "captured context must not keep host/journal alive"
    );
    assert_eq!(root.drain().err(), Some(AdmissionError::RuntimeDropped));
}

#[tokio::test(flavor = "current_thread")]
async fn running_operations_pause_resume_and_survive_dropped_waiters() {
    bounded(async {
        let runtime = Runtime::new();
        let context = runtime.context();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let mut completion = None;
        runtime
            .run(async {
                let _effect = context
                    .on_dispose(move || async move {
                        entered_tx.send(()).unwrap();
                        release_rx.await.unwrap();
                        done_tx.send(()).unwrap();
                        Err::<(), _>(UserError(9))
                    })
                    .unwrap();
                completion = Some(context.drain().unwrap());
                entered_rx.await.unwrap();
            })
            .await
            .unwrap();
        drop(completion.take().unwrap());
        release_tx.send(()).unwrap();
        runtime
            .run(async {
                done_rx.await.unwrap();
                context.drain().unwrap().await.unwrap();
            })
            .await
            .unwrap();
        let failures = runtime.take_failures();
        assert_eq!(failures.len(), 1);
        assert_eq!(
            failures[0]
                .source()
                .unwrap()
                .downcast_ref::<UserError>()
                .unwrap()
                .0,
            9
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn destroying_host_reports_cancelled_completion_not_success() {
    bounded(async {
        let runtime = Runtime::new();
        let context = runtime.context();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let mut completion = None;
        runtime
            .run(async {
                let _effect = context
                    .on_dispose(move || async move {
                        entered_tx.send(()).unwrap();
                        release_rx.await.unwrap();
                        Ok::<_, Infallible>(())
                    })
                    .unwrap();
                completion = Some(context.drain().unwrap());
                entered_rx.await.unwrap();
            })
            .await
            .unwrap();
        drop(runtime);
        assert_eq!(
            completion.take().unwrap().await.err(),
            Some(CompletionError::HostStopped)
        );
        assert!(release_tx.send(()).is_err());
        assert_eq!(
            context
                .on_dispose(|| async { Ok::<_, Infallible>(()) })
                .err(),
            Some(AdmissionError::RuntimeDropped)
        );
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_run_scope_releases_the_host_for_later_progress() {
    let runtime = Runtime::new();
    let context = runtime.context();
    let called = Rc::new(Cell::new(false));
    let mut scope = Box::pin(runtime.run(async {
        let called = called.clone();
        let _effect = context
            .on_dispose(move || async move {
                called.set(true);
                Ok::<_, Infallible>(())
            })
            .unwrap();
        std::future::pending::<()>().await;
    }));
    let waker = std::task::Waker::noop();
    assert!(
        scope
            .as_mut()
            .poll(&mut std::task::Context::from_waker(waker))
            .is_pending()
    );
    assert_eq!(runtime.run(async {}).await, Err(HostError::AlreadyRunning));
    drop(scope);
    runtime
        .run(async {
            context.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
    assert!(called.get());
}

#[tokio::test(flavor = "current_thread")]
async fn refused_manual_claim_remains_root_owned() {
    let runtime = Runtime::new();
    let context = runtime.context();
    let called = Rc::new(Cell::new(false));
    let mut effect = None;
    runtime
        .run(async {
            effect = Some(
                context
                    .on_dispose({
                        let called = called.clone();
                        move || async move {
                            called.set(true);
                            Ok::<_, Infallible>(())
                        }
                    })
                    .unwrap(),
            );
        })
        .await
        .unwrap();
    assert_eq!(
        effect.take().unwrap().dispose().err(),
        Some(AdmissionError::HostNotRunning)
    );
    runtime
        .run(async {
            assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 1);
        })
        .await
        .unwrap();
    assert!(called.get());
}

#[tokio::test(flavor = "current_thread")]
async fn callback_destructors_can_reenter_without_a_journal_borrow() {
    struct Guard(cordis_core::Context, Rc<Cell<bool>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _effect = self
                .0
                .on_dispose(|| async { Ok::<_, Infallible>(()) })
                .unwrap();
            self.1.set(true);
        }
    }
    let runtime = Runtime::new();
    let context = runtime.context();
    let dropped = Rc::new(Cell::new(false));
    runtime
        .run(async {
            let guard = Guard(context.clone(), dropped.clone());
            let _effect = context
                .on_dispose(move || async move {
                    drop(guard);
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 1);
            assert!(dropped.get());
            assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 1);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn panicking_error_formatter_does_not_erase_the_failure_or_stall_the_barrier() {
    #[derive(Debug)]
    struct BadDisplay;
    impl fmt::Display for BadDisplay {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("user formatter")
        }
    }
    impl Error for BadDisplay {}
    let runtime = Runtime::new();
    let context = runtime.context();
    runtime
        .run(async {
            let _effect = context
                .on_dispose(|| async { Err::<(), _>(BadDisplay) })
                .unwrap();
            let report = context.drain().unwrap().await.unwrap();
            assert_eq!(report.failures().len(), 1);
            assert!(report.failures()[0].source().unwrap().is::<BadDisplay>());
            assert_eq!(runtime.take_failures().len(), 1);
            assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 0);
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_of_one_runtime_cannot_acquire_another_hosts_admission() {
    let first = Runtime::new();
    let second = Runtime::new();
    let first_context = first.context();
    let second_context = second.context();
    let cleaned = Rc::new(Cell::new(false));
    second
        .run(async {
            let cleaned = cleaned.clone();
            let _effect = second_context
                .on_dispose(move || async move {
                    cleaned.set(true);
                    Ok::<_, Infallible>(())
                })
                .unwrap();
        })
        .await
        .unwrap();
    first
        .run(async {
            let _effect = first_context
                .on_dispose(move || async move {
                    assert_eq!(
                        second_context.drain().err(),
                        Some(AdmissionError::WrongHost)
                    );
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            first_context.drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
    second
        .run(async {
            second.context().drain().unwrap().await.unwrap();
        })
        .await
        .unwrap();
    assert!(cleaned.get());
}

#[tokio::test(flavor = "current_thread")]
async fn panicking_panic_payload_destructor_is_a_supervised_driver_failure() {
    struct BadPayload;
    impl Drop for BadPayload {
        fn drop(&mut self) {
            panic!("panic payload destructor failed");
        }
    }
    bounded(async {
        let runtime = Runtime::new();
        let context = runtime.context();
        runtime
            .run(async {
                let _effect = context
                    .on_dispose(|| -> std::future::Ready<Result<(), Infallible>> {
                        std::panic::panic_any(BadPayload)
                    })
                    .unwrap();
                assert_eq!(
                    context.drain().unwrap().await.err(),
                    Some(CompletionError::DriverPanicked)
                );
                assert_eq!(context.drain().unwrap().await.unwrap().attempted(), 0);
            })
            .await
            .unwrap();
    })
    .await;
}
