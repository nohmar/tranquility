use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};
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
pub struct ResponseCallback(pub Box<dyn Fn(MutexGuard<Node>) + Send + Sync + 'static>);

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
            MessageKind::BroadcastOk(message) => {
                if message.dest != node.id.clone().unwrap() {
                    eprintln!(
                        "Skipping message because the destination is the same as node: {:?}",
                        message
                    );

                    return;
                }

                if let MessageBody::BroadcastOk(body) = &message.body {
                    if let Some(response_callback) =
                        node.response_callbacks.remove(&body.in_reply_to)
                    {
                        let ResponseCallback(callback) = response_callback;
                        callback(node);

                        eprintln!("Broadcast Ok received for message: {:?}", body.in_reply_to);
                    } else {
                        eprintln!(
                            "Skipping Broadcast OK callback because the in reply to is not found in callbacks: {:?}",
                            message
                        );
                    }
                }
            }
            MessageKind::Broadcast(message) => {
                if let MessageBody::Broadcast(body) = &message.body {
                    let is_message_seen = node.messages.contains(&body.message);

                    if !is_message_seen {
                        node.messages.insert(body.message);

                        // Generate message ID, and persist the message ID in the list of
                        // unacknowledged messages before sending the first message.
                        let mapped_messages = node
                            .topology
                            .clone()
                            .into_iter()
                            .filter_map(|node_id| {
                                // Don't send the message back to the message's original src, even
                                // if the src is a neighbor.
                                if node_id == message.src.clone().unwrap() {
                                    return None;
                                }

                                Some((node_id, node.next_message_id()))
                            })
                            .collect::<Vec<(String, u32)>>();

                        // Store the message id's in the message list.
                        for (_node_id, message_id) in mapped_messages.iter() {
                            node.unacknowledged_messages
                                .lock()
                                .unwrap()
                                .insert(*message_id);
                        }

                        let retry_node = mutex.clone();
                        let body_clone = body.clone();

                        // Spawn a thread to execute `Node::retry` method in a non-async function.
                        // Calling `retry` asynchronously allows updates to unacknowledged_messages
                        // in another thread to propogate without causing the `while` to block,
                        // causing an infinite loop.
                        //
                        // NOTE: we are not `.await`ing the spawned thread; its possible the thread
                        // is still processing messages after the main thread is closed.
                        tokio::spawn(async move {
                            while Node::retry(&retry_node) {
                                let messages = Node::filter_messages(&retry_node, &mapped_messages);

                                for (node_id, message_id) in messages.into_iter() {
                                    Node::send_message(
                                        &retry_node,
                                        &body_clone,
                                        node_id,
                                        message_id,
                                    );
                                }

                                // FIXME: Add a short delay before checking messages again. This
                                // avoid blocking the thread with locks, causing net-timeouts.
                                tokio::time::sleep(std::time::Duration::from_millis(1)).await;

                                eprintln!("Messages sent. Waiting for acknowledgements...");
                            }

                            eprintln!("Acknowledged all messages.");
                        });
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

    fn retry(node: &Arc<Mutex<Node>>) -> bool {
        let node = node.lock().unwrap();
        let messages = node.unacknowledged_messages.lock().unwrap();

        eprintln!("Unacknowledged messages: {:?}", messages);
        !messages.is_empty()
    }

    fn filter_messages(
        node: &Arc<Mutex<Node>>,
        mapped_messages: &Vec<(String, u32)>,
    ) -> Vec<(String, u32)> {
        let node = node.lock().unwrap();
        let unacknowledged_messages = node.unacknowledged_messages.lock().unwrap();

        mapped_messages
            .clone()
            .into_iter()
            .filter_map(|message| {
                let (_node_id, message_id) = &message;

                if unacknowledged_messages.contains(&message_id) {
                    Some(message)
                } else {
                    None
                }
            })
            .collect()
    }

    fn send_message(
        node: &Arc<Mutex<Node>>,
        body: &BroadcastBody,
        node_id: String,
        message_id: u32,
    ) {
        let mut node = node.lock().unwrap();
        let stdout = io::stdout();
        let mut lock = stdout.lock();

        let message = Message {
            src: node.id.clone(),
            dest: node_id.to_owned(),
            body: MessageBody::Broadcast(BroadcastBody {
                r#type: "broadcast".to_owned(),
                msg_id: Some(message_id),
                in_reply_to: None,
                message: body.message,
            }),
        };

        let message = serde_json::to_string(&message).expect("Couldn't parse message.");

        // Log message to stderr.
        eprintln!("Sent {:?}", message);

        // Flush the message to stdout.
        writeln!(lock, "{}", message).unwrap();

        if !node.response_callbacks.contains_key(&message_id) {
            // Add a callback for the message, using the message id as the key.
            node.response_callbacks.insert(
                message_id,
                ResponseCallback(Box::new(move |node| {
                    let mut unlocked_messages = node.unacknowledged_messages.lock().unwrap();

                    unlocked_messages.remove(&message_id);

                    eprintln!("Callback invoked for msg: {:?}", message);
                })),
            );
        }
    }
}
