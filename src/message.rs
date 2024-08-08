use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::node::Node;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub src: Option<String>,
    pub dest: String,
    pub body: MessageBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageBody {
    Init(InitBody),
    Echo(EchoBody),
    Broadcast(BroadcastBody),
    BroadcastOk(BroadcastOkBody),
    Topology(TopologyBody),
    Read(ReadBody),
    Generate(GenerateBody),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub r#type: String,
    pub msg_id: Option<u32>,
    pub node_id: String,
    pub node_ids: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EchoBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub r#type: String,
    pub msg_id: Option<u32>,
    pub in_reply_to: Option<u32>,
    pub echo: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenerateBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub r#type: String,
    pub msg_id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BroadcastBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub r#type: String,
    pub message: u32,
    pub msg_id: Option<u32>,
    pub in_reply_to: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub r#type: String,
    pub msg_id: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopologyBody {
    pub r#type: String,
    pub topology: HashMap<String, Vec<String>>,
    pub msg_id: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub enum MessageKind {
    Init(Message),
    Echo(Message),
    Invalid(Message),
    Generate(Message),
    Broadcast(Message),
    BroadcastOk(Message),
    Read(Message),
    Topology(Message),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Response {
    InitOk(InitOkResponse),
    EchoOk(EchoOkResponse),
    GenerateOk(GenerateOkResponse),
    BroadcastOk(BroadcastOkResponse),
    ReadOk(ReadOkResponse),
    TopologyOk(TopologyOkResponse),
    Invalid(InvalidResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitOkResponse {
    src: Option<String>,
    dest: String,
    body: InitOkBody,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EchoOkResponse {
    src: Option<String>,
    dest: String,
    body: EchoOkBody,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EchoOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
    echo: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct GenerateOkResponse {
    src: Option<String>,
    dest: String,
    body: GenerateOkBody,
}

#[derive(Clone, Debug, Serialize)]
pub struct GenerateOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
    id: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BroadcastOkResponse {
    src: Option<String>,
    dest: String,
    body: BroadcastOkBody,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BroadcastOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub r#type: String,
    pub msg_id: Option<u32>,
    pub in_reply_to: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReadOkResponse {
    src: Option<String>,
    dest: String,
    body: ReadOkBody,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReadOkBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    r#type: String,
    messages: HashSet<u32>,
    msg_id: Option<u32>,
    in_reply_to: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct TopologyOkResponse {
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
pub struct InvalidResponse {
    src: Option<String>,
    dest: String,
    body: String,
}

impl Message {
    pub fn new(message: Message) -> MessageKind {
        return match message.body {
            MessageBody::Init(ref _body) => MessageKind::Init(message),
            MessageBody::Echo(ref _body) => MessageKind::Echo(message),
            MessageBody::Generate(ref _body) => MessageKind::Generate(message),
            MessageBody::Broadcast(ref _body) => MessageKind::Broadcast(message),
            MessageBody::BroadcastOk(ref _body) => MessageKind::BroadcastOk(message),
            MessageBody::Read(ref _body) => MessageKind::Read(message),
            MessageBody::Topology(ref _body) => MessageKind::Topology(message),
            _ => MessageKind::Invalid(message),
        };
    }
}

impl MessageKind {
    pub fn generate_response(
        self,
        node: &Node,
        next_message_id: u32,
    ) -> Option<(Response, Message)> {
        return match self {
            MessageKind::Init(message) => {
                let MessageBody::Init(body) = &message.body else {
                    return Some((
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    ));
                };

                Some((
                    Response::InitOk(InitOkResponse {
                        body: InitOkBody {
                            msg_id: Some(next_message_id),
                            r#type: "init_ok".to_string(),
                            in_reply_to: body.msg_id,
                        },
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                    }),
                    message,
                ))
            }
            MessageKind::Echo(message) => {
                let MessageBody::Echo(body) = &message.body else {
                    return Some((
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    ));
                };

                Some((
                    Response::EchoOk(EchoOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: EchoOkBody {
                            r#type: "echo_ok".to_string(),
                            in_reply_to: body.msg_id,
                            msg_id: Some(next_message_id),
                            echo: body.echo.to_owned(),
                        },
                    }),
                    message,
                ))
            }
            MessageKind::Generate(message) => {
                let MessageBody::Generate(body) = &message.body else {
                    return Some((
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    ));
                };

                Some((
                    Response::GenerateOk(GenerateOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: GenerateOkBody {
                            r#type: "generate_ok".to_string(),
                            in_reply_to: Some(body.msg_id),
                            msg_id: Some(next_message_id),
                            id: node.generate_uuid(
                                &message
                                    .to_owned()
                                    .src
                                    .expect("Generate message did not include a src."),
                            ),
                        },
                    }),
                    message,
                ))
            }
            MessageKind::Broadcast(message) => {
                let MessageBody::Broadcast(body) = &message.body else {
                    return Some((
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    ));
                };

                Some((
                    Response::BroadcastOk(BroadcastOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: BroadcastOkBody {
                            r#type: "broadcast_ok".to_string(),
                            msg_id: Some(next_message_id),
                            in_reply_to: body.msg_id.unwrap(),
                        },
                    }),
                    message,
                ))
            }
            MessageKind::Read(message) => {
                let MessageBody::Read(body) = &message.body else {
                    return Some((
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    ));
                };

                if body.r#type != "read" {
                    return None;
                }

                Some((
                    Response::ReadOk(ReadOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: ReadOkBody {
                            r#type: "read_ok".to_string(),
                            messages: node.messages.clone(),
                            msg_id: Some(next_message_id),
                            in_reply_to: body.msg_id.unwrap(),
                        },
                    }),
                    message,
                ))
            }
            MessageKind::Topology(message) => {
                let MessageBody::Topology(body) = &message.body else {
                    return Some((
                        Response::Invalid(InvalidResponse {
                            src: node.id.clone(),
                            dest: message.clone().src.unwrap().to_owned(),
                            body: "There was an error.".to_string(),
                        }),
                        message,
                    ));
                };

                Some((
                    Response::TopologyOk(TopologyOkResponse {
                        src: node.id.clone(),
                        dest: message.clone().src.unwrap().to_owned(),
                        body: TopologyOkBody {
                            r#type: "topology_ok".to_string(),
                            msg_id: Some(next_message_id),
                            in_reply_to: body.msg_id.unwrap(),
                        },
                    }),
                    message,
                ))
            }
            MessageKind::Invalid(message) => Some((
                Response::Invalid(InvalidResponse {
                    src: node.id.clone(),
                    dest: message.clone().src.unwrap().to_owned(),
                    body: "There was an error.".to_string(),
                }),
                message,
            )),
            MessageKind::BroadcastOk(_message) => None,
        };
    }
}
