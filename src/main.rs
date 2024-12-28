use std::io::{Seek, SeekFrom};
use std::{
    fs::File,
    io::{BufRead, BufReader, Cursor, Read},
};

use quick_xml::errors::IllFormedError;
use quick_xml::events::Event;
use tokio::join;

#[tokio::main]
async fn main() {
    let handle_one = tokio::spawn(async move {
        let file = File::open("enwiki-20241201-pages-articles-multistream.xml").unwrap();
        let reader = BufReader::new(file);
        let reader = quick_xml::Reader::from_reader(reader);

        let mut state_machine = StateMachine::default();
        state_machine.run(reader);
    });

    let handle_two = tokio::spawn(async move {
        let file = File::open("enwiki-20241201-pages-articles-multistream.xml").unwrap();
        let mut reader = BufReader::new(file);

        let middle = reader.get_ref().metadata().unwrap().len() / 2;
        reader.seek(SeekFrom::Start(middle)).unwrap();

        let reader = quick_xml::Reader::from_reader(reader);

        let mut state_machine = StateMachine::default();
        state_machine.run(reader);
    });

    let (one, two) = join!(handle_one, handle_two);
    one.unwrap();
    two.unwrap();
}

#[derive(Debug)]
enum State {
    Idle,
    FoundPage,
    FoundTitleStart,
    FoundTitleEnd,
    FoundText,
    Done,
}

impl Default for State {
    fn default() -> Self {
        Self::Idle
    }
}

#[derive(Default)]
struct StateMachine {
    state: State,
    title: Vec<u8>,
}

impl StateMachine {
    fn run(&mut self, mut reader: quick_xml::Reader<BufReader<File>>) {
        let mut buf = Vec::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Eof) => {
                    self.change_state(State::Done);
                    break;
                }
                Ok(Event::Start(bytes)) => match &self.state {
                    State::Idle => {
                        if bytes.name().into_inner() == b"page" {
                            self.change_state(State::FoundPage);
                        }
                    }
                    State::FoundPage => {
                        if bytes.name().into_inner() == b"title" {
                            self.change_state(State::FoundTitleStart);
                        }
                    }
                    State::FoundTitleEnd => {
                        if bytes.name().into_inner() == b"text" {
                            self.change_state(State::FoundText);
                        }
                    }
                    s => panic!("invalid state {s:?}"),
                },
                Ok(Event::Text(bytes)) => match &self.state {
                    State::Idle | State::FoundPage | State::FoundTitleEnd => {}
                    State::FoundTitleStart => {
                        self.title = (&bytes as &[u8]).to_vec();
                    }
                    State::FoundText => {
                        let links = self.parse_links(&bytes as &[u8]);
                        self.change_state(State::Idle);
                    }
                    s => panic!("invalid state {s:?}",),
                },
                Ok(Event::End(bytes)) => match &self.state {
                    State::Idle | State::FoundPage | State::FoundTitleEnd => {}
                    State::FoundTitleStart => {
                        if bytes.name().into_inner() == b"title" {
                            self.change_state(State::FoundTitleEnd);
                        }
                    }
                    s => panic!("invalid state {s:?}",),
                },
                Ok(_) => {}
                Err(err) => match err {
                    quick_xml::Error::IllFormed(_) => {}
                    _ => panic!("{:?}", err),
                },
            }

            buf.clear();
        }
    }

    fn change_state(&mut self, state: State) {
        self.state = state;
    }

    fn parse_links(&self, text: &[u8]) -> Vec<Link> {
        let mut cursor = Cursor::new(text);
        let mut buf = Vec::new();
        let mut links = Vec::new();

        loop {
            cursor.read_until(b'[', &mut buf).unwrap();
            buf.clear();
            if cursor.position() as usize == text.len() {
                break;
            }

            let mut bracket = vec![0; 1];
            cursor.read_exact(&mut bracket).unwrap();

            if bracket == vec![b'['] {
                cursor.read_until(b']', &mut buf).unwrap();
                let link = Link {
                    name: String::from_utf8(buf[..buf.len() - 1].to_vec()).unwrap(),
                };
                links.push(link);
            }
        }

        links
    }
}

#[derive(Debug)]
struct Link {
    name: String,
}
