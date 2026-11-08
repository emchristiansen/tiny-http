use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Manages a collection of threads.
///
/// A new thread is created every time all the existing threads are full.
/// Any idle thread will automatically die after a few seconds.
pub struct TaskPool {
    sharing: Arc<Sharing>,
}

struct Sharing {
    // list of the tasks to be done by worker threads
    todo: Mutex<VecDeque<Box<dyn FnMut() + Send>>>,

    // condvar that will be notified whenever a task is added to `todo`
    condvar: Condvar,

    // number of total worker threads running
    active_tasks: AtomicUsize,

    // number of idle worker threads
    waiting_tasks: AtomicUsize,
}

/// Minimum number of active threads.
static MIN_THREADS: usize = 4;

struct Registration<'a> {
    nb: &'a AtomicUsize,
}

impl<'a> Registration<'a> {
    fn new(nb: &'a AtomicUsize) -> Registration<'a> {
        nb.fetch_add(1, Ordering::Release);
        Registration { nb }
    }
}

impl<'a> Drop for Registration<'a> {
    fn drop(&mut self) {
        self.nb.fetch_sub(1, Ordering::Release);
    }
}

impl TaskPool {
    pub fn new() -> TaskPool {
        let pool = TaskPool {
            sharing: Arc::new(Sharing {
                todo: Mutex::new(VecDeque::new()),
                condvar: Condvar::new(),
                active_tasks: AtomicUsize::new(0),
                waiting_tasks: AtomicUsize::new(0),
            }),
        };

        for _ in 0..MIN_THREADS {
            pool.add_thread(None)
        }

        pool
    }

    /// Executes a function in a thread.
    /// If no thread is available, spawns a new one.
    pub fn spawn(&self, code: Box<dyn FnMut() + Send>) {
        let mut queue = self.sharing.todo.lock().unwrap();
        self.spawn_locked(&mut queue, code);
    }

    /// The decision `spawn` makes, taken while the caller holds `todo`.
    ///
    /// `waiting_tasks` still counts an idle thread after `notify_one` has woken it, until it
    /// retakes `todo` and pops a task, and every task already in `queue` will be taken by one of
    /// those threads. So `code` can wait in the queue only while the idle threads outnumber the
    /// tasks queued before it; otherwise it gets a new thread. Comparing with zero instead let a
    /// burst of connections queue a task that no thread would take until another connection
    /// closed.
    fn spawn_locked(
        &self,
        queue: &mut VecDeque<Box<dyn FnMut() + Send>>,
        code: Box<dyn FnMut() + Send>,
    ) {
        if queue.len() >= self.sharing.waiting_tasks.load(Ordering::Acquire) {
            self.add_thread(Some(code));
        } else {
            queue.push_back(code);
            self.sharing.condvar.notify_one();
        }
    }

    fn add_thread(&self, initial_fn: Option<Box<dyn FnMut() + Send>>) {
        let sharing = self.sharing.clone();

        thread::spawn(move || {
            let sharing = sharing;
            let _active_guard = Registration::new(&sharing.active_tasks);

            if let Some(mut f) = initial_fn {
                f();
            }

            loop {
                let mut task: Box<dyn FnMut() + Send> = {
                    let mut todo = sharing.todo.lock().unwrap();

                    let task;
                    loop {
                        if let Some(poped_task) = todo.pop_front() {
                            task = poped_task;
                            break;
                        }
                        let _waiting_guard = Registration::new(&sharing.waiting_tasks);

                        let received =
                            if sharing.active_tasks.load(Ordering::Acquire) <= MIN_THREADS {
                                todo = sharing.condvar.wait(todo).unwrap();
                                true
                            } else {
                                let (new_lock, waitres) = sharing
                                    .condvar
                                    .wait_timeout(todo, Duration::from_millis(5000))
                                    .unwrap();
                                todo = new_lock;
                                !waitres.timed_out()
                            };

                        if !received && todo.is_empty() {
                            return;
                        }
                    }

                    task
                };

                task();
            }
        });
    }
}

impl Drop for TaskPool {
    fn drop(&mut self) {
        self.sharing
            .active_tasks
            .store(999_999_999, Ordering::Release);
        self.sharing.condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskPool, MIN_THREADS};
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    /// A burst of connection tasks, each of which keeps its thread until released (as a keep-alive
    /// connection does), handed to the pool faster than its notified idle threads can retake
    /// `todo`. Holding `todo` across the whole burst makes that window certain: no notified thread
    /// can take a task, or leave the idle count, until the burst is queued. Every task must still
    /// start on its own thread while the others hold theirs.
    #[test]
    fn a_burst_handed_over_before_any_idle_thread_wakes_starts_every_task() {
        let pool = TaskPool::new();
        while pool.sharing.waiting_tasks.load(Ordering::Acquire) < MIN_THREADS {
            thread::yield_now();
        }

        let burst = MIN_THREADS + 1;
        let (started_tx, started_rx) = mpsc::channel();
        let mut releases = Vec::new();
        {
            let mut queue = pool.sharing.todo.lock().unwrap();
            for task in 0..burst {
                let started_tx = started_tx.clone();
                let (release_tx, release_rx) = mpsc::channel::<()>();
                releases.push(release_tx);
                let mut release_rx = Some(release_rx);
                pool.spawn_locked(
                    &mut queue,
                    Box::new(move || {
                        started_tx.send(task).unwrap();
                        // Held until the test drops the sender, like a connection the client
                        // keeps open.
                        let _ = release_rx.take().unwrap().recv();
                    }),
                );
            }
        }

        let mut started = Vec::new();
        for _ in 0..burst {
            // Each task starts at once when it has a thread; the bound only ends a run in which
            // one never does.
            match started_rx.recv_timeout(Duration::from_secs(10)) {
                Ok(task) => started.push(task),
                Err(_) => break,
            }
        }
        drop(releases);
        started.sort();
        assert_eq!(
            started,
            (0..burst).collect::<Vec<_>>(),
            "tasks {:?} of a {}-task burst started; the rest were left queued with no thread to run them",
            started,
            burst
        );
    }
}
