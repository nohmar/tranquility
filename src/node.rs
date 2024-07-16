use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::BufRead;
use std::io::{self, Write};
use std::time::SystemTime;

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
}

// Define the callback type and allow it to be displayed.
pub struct ResponseCallback(pub Box<dyn Fn() + 'static>);

impl fmt::Debug for ResponseCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Callback Function")
    }
}

impl Node {
    pub fn run(
        &mut self,
        input: &mut impl BufRead,
        output: &mut String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let _ = input.read_line(output);

            let serialized_message: Message = serde_json::from_str(&output)?;
            let next_message_id = self.next_message_id();

            let message = Message::new(serialized_message);

            self.run_callback(&message, next_message_id);

            if let Some((response, _original_message)) =
                message.generate_response(self, next_message_id)
            {
                let stdout = io::stdout();
                let mut lock = stdout.lock();

                writeln!(
                    lock,
                    "{}",
                    serde_json::to_string(&response).expect("Couldn't parse response.")
                )
                .unwrap();

                // Log results to stderr:
                /*eprintln!(
                 *    "Received: {}, Processed: {}",
                 *    output.clone(),
                 *    serde_json::to_string(&response).unwrap()
                 *);
                 */
            }

            // Flush all buffers
            output.clear();
            io::stdout().flush()?;
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

    pub fn run_callback(&mut self, message: &MessageKind, next_message_id: u32) {
        match message {
            MessageKind::Init(message) => {
                if let MessageBody::Init(body) = &message.body {
                    self.id = Some(body.node_id.to_owned());
                }
            }
            MessageKind::Broadcast(message) => {
                if let MessageBody::Broadcast(body) = &message.body {
                    let is_message_seen = self.messages.contains(&body.message);

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

                            // Flush the message to stdout.
                            writeln!(
                                lock,
                                "{}",
                                serde_json::to_string(&message).expect("Couldn't parse message.")
                            )
                            .unwrap();

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
                    }
                }
            }
            MessageKind::Topology(message) => {
                if let MessageBody::Topology(body) = &message.body {
                    let body_topology = body.topology.to_owned();
                    let node_id = self.id.to_owned().unwrap();

                    if let Some(topology) = body_topology.get(&node_id) {
                        self.topology = topology.to_vec();
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
