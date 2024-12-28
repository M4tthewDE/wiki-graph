use std::io::{Seek, SeekFrom};
use std::{
    fs::File,
    io::{BufRead, BufReader, Cursor, Read},
};

use quick_xml::events::Event;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, QueryBuilder};
use tokio::join;
use tokio::sync::mpsc::{Receiver, Sender};

#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect("postgres://postgres:postgres@localhost:5432/wiki")
        .await
        .unwrap();

    setup_db(&pool).await;

    let p = pool.clone();
    let handle_one = tokio::spawn(async move {
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let file = File::open("enwiki-20241201-pages-articles-multistream.xml").unwrap();
        let reader = BufReader::new(file);
        let reader = quick_xml::Reader::from_reader(reader);

        let mut state_machine = StateMachine::default();
        let reader_handle = tokio::spawn(writer(p.clone(), rx));
        state_machine.run(reader, &p, tx).await;
        reader_handle.await.unwrap();
    });

    let p = pool.clone();
    let handle_two = tokio::spawn(async move {
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let file = File::open("enwiki-20241201-pages-articles-multistream.xml").unwrap();
        let mut reader = BufReader::new(file);

        let middle = reader.get_ref().metadata().unwrap().len() / 2;
        reader.seek(SeekFrom::Start(middle)).unwrap();

        let reader = quick_xml::Reader::from_reader(reader);

        let reader_handle = tokio::spawn(writer(p.clone(), rx));
        let mut state_machine = StateMachine::default();
        state_machine.run(reader, &p, tx).await;
        reader_handle.await.unwrap();
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
    title: String,
}

impl StateMachine {
    async fn run(
        &mut self,
        mut reader: quick_xml::Reader<BufReader<File>>,
        pool: &Pool<Postgres>,
        tx: Sender<Article>,
    ) {
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
                        self.title = String::from_utf8((&bytes as &[u8]).to_vec()).unwrap();
                    }
                    State::FoundText => {
                        let links = self.parse_links(&bytes as &[u8]);
                        tx.send(Article {
                            title: self.title.clone(),
                            links,
                        })
                        .await
                        .unwrap();
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

    fn parse_links(&self, text: &[u8]) -> Vec<String> {
        let mut cursor = Cursor::new(text);
        let mut buf = Vec::new();
        let mut links = Vec::new();

        loop {
            cursor.read_until(b'[', &mut buf).unwrap();
            buf = vec![];
            if cursor.position() as usize == text.len() {
                break;
            }

            let mut bracket = vec![0; 1];
            cursor.read_exact(&mut bracket).unwrap();

            if bracket == vec![b'['] {
                cursor.read_until(b']', &mut buf).unwrap();
                links.push(String::from_utf8(buf[..buf.len() - 1].to_vec()).unwrap());
            }
        }

        links
    }
}

#[derive(Debug)]
struct Article {
    title: String,
    links: Vec<String>,
}

async fn setup_db(pool: &Pool<Postgres>) {
    sqlx::query("DROP TABLE articles")
        .execute(pool)
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE articles (
        id SERIAL PRIMARY KEY,
        title text
    )",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_articles(pool: &Pool<Postgres>, articles: &[Article]) {
    let mut query_builder = QueryBuilder::new("INSERT INTO articles (title) ");
    query_builder.push_values(articles, |mut b, article| {
        b.push_bind(article.title.clone());
    });
    query_builder.push(" ON CONFLICT DO NOTHING");

    let query = query_builder.build();
    query.execute(pool).await.unwrap();
}

async fn writer(pool: Pool<Postgres>, mut rx: Receiver<Article>) {
    let mut buffer = Vec::new();
    while let Some(article) = rx.recv().await {
        buffer.push(article);

        if buffer.len() == 1_000 {
            insert_articles(&pool, &buffer).await;
            buffer.clear();
        }
    }
}
