use std::{
    panic::{self, AssertUnwindSafe},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use super::Result;
use super::ThreadPool;

type Job = Box<dyn FnOnce() + Send + 'static>;
enum ThreadPoolMessage {
    Run(Job),
    Shutdown,
}
struct Worker {
    id: u32,
    join_handle: Option<JoinHandle<()>>,
}
impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<Receiver<ThreadPoolMessage>>>) -> Self {
        let join_handle = thread::spawn(move || loop {
            match receiver.lock() {
                Ok(receiver) => match receiver.recv() {
                    Ok(message) => match message {
                        ThreadPoolMessage::Run(job) => {
                            if let Err(e) = panic::catch_unwind(AssertUnwindSafe(job)) {
                                println!("Worker {} panicked running job {:?}", id, e);
                            }
                        }
                        ThreadPoolMessage::Shutdown => {
                            println!("Worker {} received message to shutdown", id);
                            return;
                        }
                    },
                    Err(e) => {
                        println!("Worker {} received error reading from channel: {:?}", id, e);
                    }
                },
                Err(e) => {
                    println!("Worker {} failed to lock receiver: {:?}", id, e);
                }
            }
        });
        Worker {
            id,
            join_handle: Some(join_handle),
        }
    }
}

pub struct SharedQueueThreadPool {
    workers: Vec<Worker>,
    sender: Sender<ThreadPoolMessage>,
}
impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(threads as usize);
        for i in 0..threads {
            workers.push(Worker::new(i, Arc::clone(&receiver)));
        }
        Ok(SharedQueueThreadPool { workers, sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self.sender.send(ThreadPoolMessage::Run(Box::new(job))) {
            Err(e) => {
                println!("Error sending job to worker channel: {:?}", e);
            }
            Ok(_) => {}
        }
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        if thread::panicking() {
            println!("dropped while unwinding panic");
            return;
        }
        for _ in 0..self.workers.len() {
            if let Err(e) = self.sender.send(ThreadPoolMessage::Shutdown) {
                println!("Failed to send while shutting down: {:?}", e);
            }
        }
        for worker in &mut self.workers {
            if let Some(thread) = worker.join_handle.take() {
                if let Err(e) = thread.join() {
                    println!("Failed to join while shutting down: {:?}", e);
                }
            }
        }
    }
}
