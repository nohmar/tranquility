use serde::{Deserialize, Serialize};

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
    Generate(GenerateBody),
}

#[derive(Clone, Debug, Deserialize)]
struct InitBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<i32>,
    node_id: String,
    node_ids: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EchoBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<i32>,
    in_reply_to: Option<i32>,
    echo: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GenerateBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<i32>,
}

#[derive(Debug, Deserialize)]
enum MessageKind {
    Init(Message),
    Echo(Message),
    Invalid(Message),
    Generate(Message),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Response {
    InitOk(InitOkResponse),
    EchoOk(EchoOkResponse),
    GenerateOk(GenerateOkResponse),
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
    msg_id: Option<i32>,
    in_reply_to: Option<i32>,
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
    msg_id: Option<i32>,
    in_reply_to: Option<i32>,
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
    msg_id: Option<i32>,
    in_reply_to: Option<i32>,
    id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InvalidResponse {
    src: Option<String>,
    dest: String,
    body: String,
}

struct Node {
    id: Option<String>,
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
            _ => MessageKind::Invalid(message),
        };
    }
}

impl MessageKind {
    fn generate_response(self, node: &Node) -> (Response, Message) {
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
                            msg_id: Some(body.msg_id.unwrap_or(0) + 1),
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
                            msg_id: body.msg_id.and_then(|id| Some(id + 1)),
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
                            in_reply_to: body.msg_id,
                            msg_id: body.msg_id.and_then(|id| Some(id + 1)),
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

            let (response, _original_message) = message.generate_response(&self);

            println!(
                "{}",
                serde_json::to_string(&response).expect("Couldn't parse response.")
            );

            // Flush all buffers
            output.clear();
            //io::stdout().flush()?;
        }
    }

    fn before_response_callback(&mut self, message: &Message) {
        if let MessageBody::Init(body) = &message.body {
            self.id = Some(body.node_id.to_owned());
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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = String::new();
    let mut node = Node { id: None };

    let result = node.run(&mut io::stdin().lock(), &mut output);

    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn responds_with_init_message() {
        let mut buffer = String::new();
        let mut node = Node { id: None };

        let mut message = r#"{"id": 508799, "src": "c1", "dest": "n3", "body": {"type": "init", "msg_id": 1, "in_reply_to": null, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}"#.as_bytes();

        let _ = node.run(&mut message, &mut buffer);
    }

    #[test]
    fn responds_to_generate_message() {
        let mut buffer = String::new();
        let mut node = Node {
            id: Some("n1".to_string()),
        };

        let mut message = r#"{"id": 500005, "src": "c1", "dest": "n3", "body": {"type": "generate", "msg_id": 1 }}"#.as_bytes();

        let _ = node.run(&mut message, &mut buffer);
    }
}
