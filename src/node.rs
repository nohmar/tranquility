use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::task::TaskTracker;

use crate::message::{BroadcastBody, Message, MessageBody, MessageKind};
use serde::Serialize;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, Hash, Serialize)]
struct UniqueId(String);

impl UniqueId {
    fn generate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        self.0.hash(&mut hasher);

        hasher.finish()
    }
}

#[derive(Debug, Default)]
pub struct Node {
    pub id: Option<String>,
    pub messages: HashSet<u32>,
    pub topology: Vec<String>,
    pub current_message_id: u32,
    pub response_callbacks: HashMap<u32, ResponseCallback>,
    pub unacknowledged_messages: Arc<Mutex<HashSet<u32>>>,
}

// Define the callback type and allow it to be displayed.
pub struct ResponseCallback(pub Box<dyn Fn() + Send + Sync + 'static>);

impl fmt::Debug for ResponseCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Callback Function")
    }
}

impl Node {
    pub async fn run(
        node: Arc<Mutex<Node>>,
        mut rx: Receiver<String>,
        response_tx: Sender<String>,
        task_tracker: &TaskTracker,
    ) -> () {
        // `recv()` keeps the `rx` alive because it doesn't drop the value by ending the
        // execution of the thread. The thread is put to sleep until the channel is closed.
        //
        // You must explicitly drop `tx`, or close the thread after tracker.spawn() to close the
        // channel, and break the loop.
        while let Some(from_stdin) = rx.recv().await {
            let response_reference = response_tx.clone();

            // NOTE: node_clone must occur in the `while` loop (not outside of it), else the borrow checker
            // complains because the spawned thread takes ownership of it.
            //
            // You can't clone in the spawned thread because the thread will own `node`.
            let node_clone = node.clone();

            task_tracker.spawn(async move {
                match Node::handle_from_stdin(node_clone, &from_stdin) {
                    Ok(Some(stringified_response)) => {
                        eprintln!("Sending message: {:?}", stringified_response);
                        response_reference.send(stringified_response).await.unwrap();
                    }
                    Ok(None) => {}
                    Err(err) => {
                        eprintln!(
                            "Uh oh. Something went wrong handling stdin: {:?}, message: {:?}",
                            err, &from_stdin
                        );
                    }
                };
            });
        }

        eprintln!("{}", "Shutting down...");
    }

    pub fn handle_from_stdin(
        node: Arc<Mutex<Node>>,
        value: &String,
    ) -> Result<Option<String>, String> {
        let Ok(serialized_message) = serde_json::from_str(&value) else {
            return Err("Uh-oh, unable to parse that message.".to_string());
        };

        let message = Message::new(serialized_message);

        Node::run_callback(&node, &message);

        // Lock the mutex after `run_callback`, or else you get a deadlock;
        // `run_callback` locks the node.
        let mut locked = node.lock().unwrap();

        let id = locked.next_message_id();

        if let Some((response, _original_message)) = message.generate_response(&locked, id) {
            let stringified_response =
                serde_json::to_string(&response).expect("Couldn't parse response.");

            Ok(Some(stringified_response))
        } else {
            Ok(None)
        }
    }

    pub fn generate_uuid(&self, client_id: &String) -> u64 {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        if let Some(ref id) = self.id {
            UniqueId(format!("{}-{}-{}", id, client_id, time.as_nanos())).generate_hash()
        } else {
            panic!()
        }
    }

    pub fn next_message_id(&mut self) -> u32 {
        self.current_message_id += 1;
        self.current_message_id
    }

    pub fn run_callback(mutex: &Arc<Mutex<Node>>, message: &MessageKind) {
        let mut node = mutex.lock().unwrap();

        match message {
            MessageKind::Init(message) => {
                if let MessageBody::Init(body) = &message.body {
                    node.id = Some(body.node_id.to_owned());
                }
            }
            MessageKind::Broadcast(message) => {
                if let MessageBody::Broadcast(body) = &message.body {
                    let is_message_seen = node.messages.contains(&body.message);

                    if body.in_reply_to.is_some() {
                        if let Some(response_callback) =
                            self.response_callbacks.remove(&body.in_reply_to.unwrap())
                        {
                            let ResponseCallback(callback) = response_callback;
                            callback()
                        }
                    }

                    if !is_message_seen {
                        self.messages.insert(body.message);

                        // Broadcast the message to every known node, expect to itself, and the one that sent the message.
                        for node_id in self.topology.clone().iter() {
                            if *node_id == message.src.clone().unwrap() {
                                continue;
                            }

                            let stdout = io::stdout();
                            let mut lock = stdout.lock();

                            let body_clone = body.clone();

                            let message = Message {
                                src: self.id.clone(),
                                dest: node_id.to_owned(),
                                body: MessageBody::Broadcast(BroadcastBody {
                                    r#type: "broadcast".to_owned(),
                                    msg_id: Some(next_message_id),
                                    in_reply_to: None,
                                    message: body_clone.message,
                                }),
                            };

                            let message =
                                serde_json::to_string(&message).expect("Couldn't parse message.");

                            // Log message to stderr.
                            eprintln!("Sent {:?}", message);

                            // Flush the message to stdout.
                            writeln!(lock, "{}", message).unwrap();

                            // Add a callback for the message, using the message id as the key.
                            self.response_callbacks.insert(
                                next_message_id,
                                ResponseCallback(Box::new(move || {
                                    eprintln!(
                                        "Callback invoked for msg: {}",
                                        body_clone.msg_id.unwrap()
                                    );
                                })),
                            );
                        }
                    } else {
                        // Log message to stderr.
                        eprintln!(
                            "Message seen {:?} - acknowledging it, but do nothing.",
                            message
                        );
                    }
                }
            }
            MessageKind::Topology(message) => {
                if let MessageBody::Topology(body) = &message.body {
                    let body_topology = body.topology.to_owned();
                    let node_id = node.id.to_owned().unwrap();

                    if let Some(topology) = body_topology.get(&node_id) {
                        node.topology = topology.to_vec();

                        eprintln!("My neighbors are: {:?}", node.topology);
                    }
                }
            }
            MessageKind::Read(_message) => (),
            MessageKind::Generate(_message) => (),
            MessageKind::Invalid(_message) => (),
            MessageKind::Echo(_message) => (),
        }
    }
}
