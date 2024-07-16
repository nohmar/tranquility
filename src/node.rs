use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::BufRead;
use std::io::{self, Write};
use std::time::SystemTime;

use crate::message::Message;
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

            let message = Message::new(serialized_message);
            message.run_callback(self);

            if let Some((response, _original_message)) = message.generate_response(self) {
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
}
