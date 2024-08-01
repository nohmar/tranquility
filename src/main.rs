mod message;
mod node;

use node::Node;
use std::sync::{Arc, Mutex};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Initialize the channel used to send messages from stdin to the node instance.
    let (tx, rx) = mpsc::channel(32);

    // Initialize the response channel;
    let (response_tx, mut response_rx) = mpsc::channel(10);

    let node = Node {
        id: None,
        ..Default::default()
    };

    let tracker = TaskTracker::new();
    let tracker_clone = tracker.clone();

    let buf_reader = BufReader::new(stdin());
    let mut lines = buf_reader.lines();

    let node = Arc::new(Mutex::new(node));

    // `Node::run` must be executed in a thread; calling `.await` immediately blocks the execution
    // of the main thread i.e. the code after it never executes -- there will be no listener on
    // stdin.
    //
    // Alternatively, use `tracker.spawn` instead of `tokio::spawn` to ensure the threads finish
    // execution after the channel is closed.
    //
    // Note, without `await`ing the join handler, a thread via `tokio::spawn` is invoked and closed
    // immediately.
    let node_handler = tokio::spawn(async move {
        // `node` must implement the `Copy` trait, but it can't because of the trait objects on the
        // Response callbacks. Therefore, `run` must be a method that takes ownership of the Node
        // instance as a `ref` gets copied during `run`s function call.
        Node::run(node, rx, response_tx, &tracker_clone).await;
    });

    let response_handler = tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
            // Log to stderr.
            eprintln!("Sent: {}", response);

            // Output the response.
            println!("{}", response);
        }
    });

    let stdin_handler = tokio::spawn(async move {
        while let Ok(wrapped_data) = lines.next_line().await {
            if let Some(data) = wrapped_data {
                tx.send(data).await.expect("Channel closed.");
            } else {
                // NOTE: Call `drop` explicitly as this breaks the Node's `while` loop during `run()`.
                // Alternatively, the `tx` can get dropped automatically if the entire loop is call in
                // a separate tracker thread.
                drop(tx);
                break;
            }
        }
    });

    let _ = node_handler.await;
    let _ = response_handler.await;
    let _ = stdin_handler.await;

    tracker.close();
    tracker.wait().await;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn responds_with_init_message() {
        let (tx, rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);

        let tracker = TaskTracker::new();
        let tracker = tracker.clone();

        let node = Arc::new(Mutex::new(Node {
            id: None,
            ..Default::default()
        }));

        let message = r#"{"id": 508799, "src": "c1", "dest": "n3", "body": {"type": "init", "msg_id": 1, "in_reply_to": null, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}"#.to_string();
        //{"id": 508799, "src": "c1", "dest": "n3", "body": {"type": "init", "msg_id": 1, "in_reply_to": null, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}

        let _ = tx.send(message).await;

        let handler = tokio::spawn(async move {
            Node::run(node, rx, response_tx, &tracker).await;
        });

        drop(tx);

        let _ = handler.await;
    }

    #[tokio::test]
    async fn responds_to_generate_message() {
        let (tx, rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);

        let tracker = TaskTracker::new();
        let tracker = tracker.clone();

        let node = Arc::new(Mutex::new(Node {
            id: None,
            ..Default::default()
        }));

        let message = r#"{"id": 500005, "src": "c1", "dest": "n3", "body": {"type": "generate", "msg_id": 1 }}"#.to_string();

        let _ = tx.send(message).await;

        let handler = tokio::spawn(async move {
            Node::run(node, rx, response_tx, &tracker).await;
        });

        drop(tx);

        let _ = handler.await;
    }

    #[tokio::test]
    async fn responds_to_broadcast_message() {
        let (tx, rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);

        let tracker = TaskTracker::new();
        let tracker = tracker.clone();

        let node = Arc::new(Mutex::new(Node {
            id: None,
            ..Default::default()
        }));

        let message =
            r#"{"id": 508799, "src": "c1", "dest": "n3", "body": {"type": "init", "msg_id": 1, "in_reply_to": null, "node_id": "n1", "node_ids": ["n1", "n2", "n3"]}}
            {"id": 100000, "src": "c1", "dest": "n3", "body": {"type": "topology", "msg_id": 1, "topology": { "n1": ["n2", "n3"], "n2": ["n1"], "n3": ["n1"] } }}
            {"id": 100000, "src": "c1", "dest": "n3", "body": {"type": "broadcast", "message": 1000, "msg_id": 1 }}"#.to_string();

        let _ = tx.send(message).await;

        let handler = tokio::spawn(async move {
            Node::run(node, rx, response_tx, &tracker).await;
        });

        drop(tx);

        let _ = handler.await;
    }

    #[tokio::test]
    async fn responds_to_read_message() {
        let (tx, rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);

        let tracker = TaskTracker::new();
        let tracker = tracker.clone();

        let node = Arc::new(Mutex::new(Node {
            id: None,
            ..Default::default()
        }));

        let message =
            r#"{"id": 100000, "src": "c1", "dest": "n3", "body": { "type": "read", "msg_id": 1 }}"#
                .to_string();

        let _ = tx.send(message).await;

        let handler = tokio::spawn(async move {
            Node::run(node, rx, response_tx, &tracker).await;
        });

        drop(tx);

        let _ = handler.await;
    }

    #[tokio::test]
    async fn responds_to_topology_message() {
        let (tx, rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);

        let tracker = TaskTracker::new();
        let tracker = tracker.clone();

        let node = Arc::new(Mutex::new(Node {
            id: None,
            ..Default::default()
        }));

        let message =
            r#"{"id": 100000, "src": "c1", "dest": "n3", "body": {"type": "topology", "msg_id": 1, "topology": { "n1": ["n2", "n3"], "n2": ["n1"], "n3": ["n1"] } }}"#.to_string();

        let _ = tx.send(message).await;

        let handler = tokio::spawn(async move {
            Node::run(node, rx, response_tx, &tracker).await;
        });

        drop(tx);

        let _ = handler.await;
    }
}
