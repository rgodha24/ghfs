//! Deduplicate concurrent async work by key: the first caller runs the future,
//! everyone else arriving while it's in flight awaits the same result.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tokio::sync::OnceCell;

pub struct SingleFlight<K, V> {
    inflight: Mutex<HashMap<K, Arc<OnceCell<V>>>>,
}

impl<K, V> Default for SingleFlight<K, V> {
    fn default() -> Self {
        Self {
            inflight: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> SingleFlight<K, V> {
    pub async fn run<F, Fut>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V>,
    {
        let cell = {
            let mut map = self.inflight.lock().expect("singleflight poisoned");
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let value = cell.get_or_init(f).await.clone();
        // Forget the result so the next call re-checks and can retry failures.
        let mut map = self.inflight.lock().expect("singleflight poisoned");
        if map.get(&key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
            map.remove(&key);
        }
        value
    }

    /// Run one piece of work on behalf of several keys: callers of [`run`]
    /// on any of them while it's in flight wait for it. Keys already in
    /// flight keep their own work.
    ///
    /// [`run`]: SingleFlight::run
    pub async fn run_group<F, Fut>(&self, keys: &[K], f: F) -> V
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V>,
    {
        let cell = Arc::new(OnceCell::new());
        {
            let mut map = self.inflight.lock().expect("singleflight poisoned");
            for key in keys {
                map.entry(key.clone()).or_insert_with(|| cell.clone());
            }
        }
        let value = cell.get_or_init(f).await.clone();
        let mut map = self.inflight.lock().expect("singleflight poisoned");
        for key in keys {
            if map.get(key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                map.remove(key);
            }
        }
        value
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inflight
            .lock()
            .expect("singleflight poisoned")
            .contains_key(key)
    }

    pub fn keys(&self) -> Vec<K> {
        self.inflight
            .lock()
            .expect("singleflight poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn concurrent_callers_share_one_run() {
        let flight = Arc::new(SingleFlight::<u32, u32>::default());
        let runs = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let flight = flight.clone();
            let runs = runs.clone();
            tasks.push(tokio::spawn(async move {
                flight
                    .run(1, || async {
                        runs.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        7
                    })
                    .await
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), 7);
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert!(flight.keys().is_empty());
    }

    #[tokio::test]
    async fn group_members_join_the_group_run() {
        let flight = Arc::new(SingleFlight::<u32, u32>::default());
        let runs = Arc::new(AtomicUsize::new(0));
        let group = {
            let flight = flight.clone();
            let runs = runs.clone();
            tokio::spawn(async move {
                flight
                    .run_group(&[1, 2, 3], || async {
                        runs.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        9
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(flight.contains(&2));
        let member = flight
            .run(2, || async {
                runs.fetch_add(1, Ordering::SeqCst);
                0
            })
            .await;
        assert_eq!(member, 9);
        assert_eq!(group.await.unwrap(), 9);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert!(flight.keys().is_empty());
    }

    #[tokio::test]
    async fn later_calls_rerun() {
        let flight = SingleFlight::<u32, u32>::default();
        assert_eq!(flight.run(1, || async { 1 }).await, 1);
        assert_eq!(flight.run(1, || async { 2 }).await, 2);
    }
}
