mod message;
mod node;

use node::Node;
use std::io;

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
            r#"{"id": 508799, "src": "c1", "dest": "n3", "body": {"type": "init", "msg_id": 1, "in_reply_to": null, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
            {"id": 100000, "src": "c1", "dest": "n3", "body": {"type": "topology", "msg_id": 1, "topology": { "n1": ["n2", "n3"], "n2": ["n1"], "n3": ["n1"] } }}
            {"id": 100000, "src": "c1", "dest": "n3", "body": {"type": "broadcast", "message": 1000, "msg_id": 1 }}"#.as_bytes();

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
