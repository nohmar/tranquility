use std::io::{self, BufRead};

struct Message {
    src: String,
    dest: String,
    body: Body,
}

struct Body {
    r#type: String,
    msg_id: Option<i32>,
    in_reply_to: Option<i32>,
}

fn main() -> io::Result<()> {
    let stack = 0;

    loop {
        let mut buffer = String::new();
        let stdin = io::stdin();
        let mut handle = stdin.lock();

        handle.read_line(&mut buffer)?;
        //println!("From STDIN: {}", buffer);
    }
}
