use serde::{Deserialize, Serialize};

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::SystemTime;

use std::io::{self, BufRead, BufReader, Read};

#[derive(Clone, Debug, Deserialize)]
struct Message {
    src: Option<String>,
    dest: String,
    body: MessageBody,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum MessageBody {
    Init(InitBody),
    Echo(EchoBody),
    Broadcast(BroadcastBody),
    Topology(TopologyBody),
    Read(ReadBody),
    Generate(GenerateBody),
}

#[derive(Clone, Debug, Deserialize)]
struct InitBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    node_id: String,
    node_ids: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
struct EchoBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
    echo: String,
}

#[derive(Clone, Debug, Deserialize)]
struct GenerateBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: u32,
}

#[derive(Clone, Debug, Deserialize)]
struct BroadcastBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    message: u32,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
struct ReadBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
struct TopologyBody {
    r#type: String,
    topology: HashMap<String, Vec<String>>,
    msg_id: Option<u32>,
}

#[derive(Debug, Deserialize)]
enum MessageKind {
    Init(Message),
    Echo(Message),
    Invalid(Message),
    Generate(Message),
    Broadcast(Message),
    Read(Message),
    Topology(Message),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Response {
    InitOk(InitOkResponse),
    EchoOk(EchoOkResponse),
    GenerateOk(GenerateOkResponse),
    BroadcastOk(BroadcastOkResponse),
    ReadOk(ReadOkResponse),
    TopologyOk(TopologyOkResponse),
    Invalid(InvalidResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InitOkResponse {
    src: Option<String>,
    dest: String,
    body: InitOkBody,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InitOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EchoOkResponse {
    src: Option<String>,
    dest: String,
    body: EchoOkBody,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EchoOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
    echo: String,
}

#[derive(Clone, Debug, Serialize)]
struct GenerateOkResponse {
    src: Option<String>,
    dest: String,
    body: GenerateOkBody,
}

#[derive(Clone, Debug, Serialize)]
struct GenerateOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
    id: u64,
}

#[derive(Clone, Debug, Serialize)]
struct BroadcastOkResponse {
    src: Option<String>,
    dest: String,
    body: BroadcastOkBody,
}

#[derive(Clone, Debug, Serialize)]
struct BroadcastOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: u32,
}

#[derive(Clone, Debug, Serialize)]
struct ReadOkResponse {
    src: Option<String>,
    dest: String,
    body: ReadOkBody,
}

#[derive(Clone, Debug, Serialize)]
struct ReadOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    messages: HashSet<u32>,
    msg_id: Option<u32>,
    in_reply_to: u32,
}

#[derive(Clone, Debug, Serialize)]
struct TopologyOkResponse {
    src: Option<String>,
    dest: String,
    body: TopologyOkBody,
}

#[derive(Clone, Debug, Serialize)]
struct TopologyOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    in_reply_to: u32,
    msg_id: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InvalidResponse {
    src: Option<String>,
    dest: String,
    body: String,
}

#[derive(Debug, Default)]
struct Node {
    id: Option<String>,
    messages: HashSet<u32>,
    topology: HashMap<String, Vec<String>>,
    current_message_id: u32,
    response_callbacks: HashMap<u32, ResponseCallback>,
}

// Define the callback type and allow it to be displayed.
struct ResponseCallback(Box<dyn Fn() + 'static>);

impl fmt::Debug for ResponseCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Callback Function")
    }
}

#[derive(Clone, Hash, Serialize)]
struct UniqueId(String);

impl UniqueId {
    fn generate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        self.0.hash(&mut hasher);

        hasher.finish()
    }
}

impl Message {
    fn new(message: Message) -> MessageKind {
        return match message.body {
            MessageBody::Init(ref _body) => MessageKind::Init(message),
            MessageBody::Echo(ref _body) => MessageKind::Echo(message),
            MessageBody::Generate(ref _body) => MessageKind::Generate(message),
            MessageBody::Broadcast(ref _body) => MessageKind::Broadcast(message),
            MessageBody::Read(ref _body) => MessageKind::Read(message),
            MessageBody::Topology(ref _body) => MessageKind::Topology(message),
            _ => MessageKind::Invalid(message),
        };
    }
}

impl MessageKind {
    fn generate_response(self, node: &mut Node) -> (Response, Message) {
        return match self {
            MessageKind::Init(message) => {
                let MessageBody::Init(body) = &message.body else {
                    return (
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    );
                };

                (
                    Response::InitOk(InitOkResponse {
                        body: InitOkBody {
                            msg_id: Some(node.next_message_id()),
                            r#type: "init_ok".to_string(),
                            in_reply_to: body.msg_id,
                        },
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                    }),
                    message,
                )
            }
            MessageKind::Echo(message) => {
                let MessageBody::Echo(body) = &message.body else {
                    return (
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    );
                };

                (
                    Response::EchoOk(EchoOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: EchoOkBody {
                            r#type: "echo_ok".to_string(),
                            in_reply_to: body.msg_id,
                            msg_id: Some(node.next_message_id()),
                            echo: body.echo.to_owned(),
                        },
                    }),
                    message,
                )
            }
            MessageKind::Generate(message) => {
                let MessageBody::Generate(body) = &message.body else {
                    return (
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    );
                };

                (
                    Response::GenerateOk(GenerateOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: GenerateOkBody {
                            r#type: "generate_ok".to_string(),
                            in_reply_to: Some(body.msg_id),
                            msg_id: Some(node.next_message_id()),
                            id: node.generate_uuid(
                                &message
                                    .to_owned()
                                    .src
                                    .expect("Generate message did not include a src."),
                            ),
                        },
                    }),
                    message,
                )
            }
            MessageKind::Broadcast(message) => {
                let MessageBody::Broadcast(body) = &message.body else {
                    return (
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    );
                };

                (
                    Response::BroadcastOk(BroadcastOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: BroadcastOkBody {
                            r#type: "broadcast_ok".to_string(),
                            msg_id: Some(node.next_message_id()),
                            in_reply_to: body.msg_id.unwrap(),
                        },
                    }),
                    message,
                )
            }
            MessageKind::Read(message) => {
                let MessageBody::Read(body) = &message.body else {
                    return (
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    );
                };

                (
                    Response::ReadOk(ReadOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: ReadOkBody {
                            r#type: "read_ok".to_string(),
                            messages: node.messages.clone(),
                            msg_id: Some(node.next_message_id()),
                            in_reply_to: body.msg_id.unwrap(),
                        },
                    }),
                    message,
                )
            }
            MessageKind::Topology(message) => {
                let MessageBody::Topology(body) = &message.body else {
                    return (
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    );
                };

                (
                    Response::TopologyOk(TopologyOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: TopologyOkBody {
                            r#type: "topology_ok".to_string(),
                            msg_id: Some(node.next_message_id()),
                            in_reply_to: body.msg_id.unwrap(),
                        },
                    }),
                    message,
                )
            }
            MessageKind::Invalid(message) => (
                Response::Invalid(InvalidResponse {
                    src: node.id.clone(),
                    dest: message.clone().src.unwrap().to_owned(),
                    body: "There was an error.".to_string(),
                }),
                message,
            ),
        };
    }
}

impl Node {
    fn run(
        &mut self,
        input: &mut impl BufRead,
        output: &mut String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{self, Write};

        loop {
            let _ = input.read_line(output);

            let serialized_message: Message = serde_json::from_str(&output)?;
            self.before_response_callback(&serialized_message);

            let message = Message::new(serialized_message);

            let (response, _original_message) = message.generate_response(self);

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

            // Flush all buffers
            output.clear();
            io::stdout().flush()?;
        }
    }

    fn before_response_callback(&mut self, message: &Message) {
        if let MessageBody::Init(body) = &message.body {
            self.id = Some(body.node_id.to_owned());
        }

        if let MessageBody::Broadcast(body) = &message.body {
            let body_clone = body.clone();

            self.messages.insert(body.message);

            // Add a callback for the message, using the message id as the key.
            self.response_callbacks.insert(
                body_clone.msg_id.unwrap(),
                ResponseCallback(Box::new(move || {
                    eprintln!(
                        "Callback invoked for msg: {}",
                        body_clone.clone().msg_id.unwrap()
                    );
                })),
            );

            if body.in_reply_to.is_some() {
                if let Some(response_callback) =
                    self.response_callbacks.remove(&body.in_reply_to.unwrap())
                {
                    let ResponseCallback(callback) = response_callback;
                    callback()
                }
            }
        }

        if let MessageBody::Topology(body) = &message.body {
            self.topology = body.topology.to_owned();
        }
    }

    fn generate_uuid(&self, client_id: &String) -> u64 {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        if let Some(ref id) = self.id {
            UniqueId(format!("{}-{}-{}", id, client_id, time.as_nanos())).generate_hash()
        } else {
            panic!()
        }
    }

    fn next_message_id(&mut self) -> u32 {
        self.current_message_id += 1;
        self.current_message_id
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = String::new();
    let mut node = Node {
        id: None,
        ..Default::default()
    };

    let result = node.run(&mut io::stdin().lock(), &mut output);

    //eprintln!("Result: {:?}", &result);

    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn responds_with_init_message() {
        let mut buffer = String::new();
        let mut node = Node {
            id: None,
            ..Default::default()
        };

        let mut message =
            r#"{"id": 508799, "src": "c1", "dest": "n3", "body": {"type": "init", "msg_id": 1, "in_reply_to": null, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}"#.as_bytes();
        //{"id": 508799, "src": "c1", "dest": "n3", "body": {"type": "init", "msg_id": 1, "in_reply_to": null, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}

        let _ = node.run(&mut message, &mut buffer);
    }

    #[test]
    fn responds_to_generate_message() {
        let mut buffer = String::new();
        let mut node = Node {
            id: Some("n1".to_string()),
            ..Default::default()
        };

        let mut message =
            r#"{"id": 500005, "src": "c1", "dest": "n3", "body": {"type": "generate", "msg_id": 1 }}"#.as_bytes();

        let _ = node.run(&mut message, &mut buffer);
    }

    #[test]
    fn responds_to_broadcast_message() {
        let mut buffer = String::new();
        let mut node = Node {
            id: Some("n1".to_string()),
            ..Default::default()
        };

        let mut message =
            r#"{"id": 100000, "src": "c1", "dest": "n3", "body": {"type": "broadcast", "message": 1000, "msg_id": 1 }}"#.as_bytes();

        let _ = node.run(&mut message, &mut buffer);
    }

    #[test]
    fn responds_to_read_message() {
        let mut buffer = String::new();
        let mut node = Node {
            id: Some("n1".to_string()),
            ..Default::default()
        };

        let mut message =
            r#"{"id": 100000, "src": "c1", "dest": "n3", "body": { "type": "read", "msg_id": 1 }}"#
                .as_bytes();

        let _ = node.run(&mut message, &mut buffer);
    }

    #[test]
    fn responds_to_topology_message() {
        let mut buffer = String::new();
        let mut node = Node {
            id: Some("n1".to_string()),
            ..Default::default()
        };

        let mut message =
            r#"{"id": 100000, "src": "c1", "dest": "n3", "body": {"type": "topology", "msg_id": 1, "topology": { "n1": ["n2", "n3"], "n2": ["n1"], "n3": ["n1"] } }}"#.as_bytes();

        let _ = node.run(&mut message, &mut buffer);
    }
}
