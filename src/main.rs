use std::{
    fs::File,
    io::{BufRead, BufReader, Cursor, Read},
};

use quick_xml::events::Event;

#[tokio::main]
async fn main() {
    let reader =
        quick_xml::Reader::from_file("enwiki-20241201-pages-articles-multistream.xml").unwrap();

    let mut state_machine = StateMachine::default();
    state_machine.run(reader);
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
                        //dbg!(String::from_utf8(self.title.clone()).unwrap());
                        let links = self.parse_links(&bytes as &[u8]);
                        //dbg!(links);
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
                Err(err) => panic!("{}", err),
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
