use std::io::Cursor;

use ciborium::{de::from_reader, value};
use cid::Cid;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio_tungstenite::connect_async;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct Commit {
    pub seq: i64,
    pub rebase: bool,
    pub too_big: bool,
    pub repo: String,
    pub commit: Cid,
    pub prev: Option<Cid>,

    // CAR file containing relevant blocks
    pub blocks: ByteBuf,
    pub ops: Vec<RepoOp>,

    pub blobs: Vec<Cid>,
    pub time: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct RepoOp {
    pub action: String,
    pub path: String,
    pub cid: Option<Cid>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Header {
    pub t: String,
}

#[tokio::main]
async fn main() {
    let connect_addr = "wss://bsky.social/xrpc/com.atproto.sync.subscribeRepos".to_owned();
    let url = url::Url::parse(&connect_addr).unwrap();
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let (_, reader) = ws_stream.split();

    reader
        .for_each(|message| async {
            let data = message.unwrap().into_data();
            let mut reader = Cursor::new(data);

            let header: Header = from_reader(&mut reader).unwrap();

            match header.t.as_str() {
                "#commit" => {
                    let commit: Commit = from_reader(&mut reader).unwrap();

                    println!(
                        "time: {} commit: {}",
                        commit.time,
                        commit.commit.into_v1().unwrap(),
                    );

                    for op in commit.ops {
                        println!("op: {:?}", op);
                    }
                }
                _ => {
                    println!("UNKNOWN HEADER: {}", header.t);
                }
            }
        })
        .await;
}
