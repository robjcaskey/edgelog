use std::thread;
use std::time::Duration;

fn main() {
    let lines = [
        "spam spam spam spam",
        "lovely spam",
        "wonderful spam",
        "spam spam eggs spam",
    ];

    let mut index = 0;

    loop {
        println!("{}", lines[index % lines.len()]);
        index += 1;
        thread::sleep(Duration::from_millis(250));
    }
}
