use std::thread;

use super::Result;
use super::ThreadPool;
pub struct NaiveThreadPool {}
impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        println!(
            "Naive thread pool will just spin up unlimited threads regardless of param {}",
            threads
        );
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
