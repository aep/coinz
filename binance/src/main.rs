extern crate websocket;
extern crate futures;
extern crate tokio_core;
extern crate hyper;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate hyper_tls;

use std::thread;
use std::io::stdin;
use websocket::result::WebSocketError;
use websocket::{ClientBuilder, OwnedMessage};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use hyper::client::Client;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_core::reactor::Core;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc;


#[derive(Serialize, Deserialize, Debug)]
struct DepthEvent {
    #[serde(rename="e")]
    event: String,
    #[serde(rename="E")]
    time: u64,
    #[serde(rename="s")]
    symbol: String,
    #[serde(rename="u")]
    update_id_end:   u64,
    #[serde(rename="U")]
    update_id_start: u64,
    #[serde(rename="b")]
    bids: Vec<(String,String,Vec<String>)>,
    #[serde(rename="a")]
    asks: Vec<(String,String,Vec<String>)>,
}

#[derive(Serialize, Deserialize, Debug)]
struct DepthResponse {
    #[serde(rename="lastUpdateId")]
    last_update_id: u64,
    bids: Vec<(String,String,Vec<String>)>,
    asks: Vec<(String,String,Vec<String>)>,
}

#[derive(PartialEq,PartialOrd)]
struct F64 (f64);
impl F64 {
    fn as_f64(&self) -> f64 {
        let &F64(f) = self;
        return f;
    }
}
impl Eq for F64 {
}
impl Ord for F64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}


#[derive(Default)]
struct OrderBook {
    update_id:  u64,
    ask:        BTreeMap<F64, (F64, u64)>,
    bid:        BTreeMap<F64, (F64, u64)>,
}

fn main() {
    let symbol = "ETHBTC";

    let api_url = format!("https://api.binance.com/api/v1/depth?symbol={}", symbol);
    let ws_url  = format!("wss://stream.binance.com:9443/ws/{}@depth?depth=1", symbol.to_lowercase());


    let mut core = Core::new().unwrap();

    let mut orderbook = OrderBook::default();

    let client = ::hyper::Client::configure()
        .connector(::hyper_tls::HttpsConnector::new(4, &core.handle()).unwrap())
        .build(&core.handle());

    let res = client.get(api_url.parse().unwrap()).and_then(|res|{
        assert_eq!(res.status(), hyper::Ok);
        res.body().concat2().and_then(move |body| {
            let e: DepthResponse = serde_json::from_slice(&body).map_err(|e|{
                std::io::Error::new(std::io::ErrorKind::Other,e)})?;
            Ok((e))
        })
    });
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 * 1000;
    let e = core.run(res).unwrap();
    for a in &e.asks {
        let price  = a.0.parse().unwrap();
        let amount = a.1.parse().unwrap();
        orderbook.ask.insert(F64(price), (F64(amount), ts as u64));
    }
    for a in &e.bids {
        let price  = a.0.parse().unwrap();
        let amount = a.1.parse().unwrap();
        orderbook.bid.insert(F64(price), (F64(amount), ts as u64));
    }




    let mut client = ClientBuilder::new(&ws_url)
        .unwrap()
        .connect(None)
        .unwrap();
    println!("[{}] connected to wsb", symbol);


    let runner = ClientBuilder::new(&ws_url)
        .unwrap()
        .async_connect(None, &core.handle())
        .and_then(|(duplex, _)|{
            let (sink, stream) = duplex.split();
            stream.filter_map(|message| {
                println!("\x1b[2J");
                println!("orderbook::update_id {}", orderbook.update_id);
                let rr = match message {
                    OwnedMessage::Ping(d)   => Some(OwnedMessage::Pong(d)),
                    OwnedMessage::Close(e)  => Some(OwnedMessage::Close(e)),
                    OwnedMessage::Text(s)   => {
                        println!("{:?}", s);
                        match serde_json::from_str::<DepthEvent>(&s) {
                            Ok(e)  => {
                                for a in e.asks {
                                    let price  = a.0.parse().unwrap();
                                    let amount = a.1.parse().unwrap();
                                    if amount == 0.0 {
                                        orderbook.ask.remove(&F64(price));
                                    } else {
                                        orderbook.ask.insert(F64(price), (F64(amount), e.time));
                                    }
                                }
                                for a in e.bids {
                                    let price  = a.0.parse().unwrap();
                                    let amount = a.1.parse().unwrap();
                                    if amount == 0.0 {
                                        orderbook.bid.remove(&F64(price));
                                    } else {
                                        orderbook.bid.insert(F64(price), (F64(amount), e.time));
                                    }
                                }
                                if orderbook.update_id != 0 &&  e.update_id_start != orderbook.update_id +1 {
                                    println!("MISSED {} UPDATES", e.update_id_start - orderbook.update_id);
                                    ::std::process::exit(3);
                                }
                                orderbook.update_id = e.update_id_end;
                            },
                            Err(e) => {
                                println!("[{}]: {}", symbol, e)
                            },
                        };
                        None
                    },
                    _ => None,
                };

                let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 * 1000;
                for e in orderbook.ask.iter().take(20).collect::<Vec<(&F64,&(F64,u64))>>().iter().rev() {
                    println!("[{}] ASK {:>20.6} {:>20.6} {}", symbol, e.0.as_f64(), (e.1).0.as_f64(), ts - (e.1).1 as i64);
                }
                println!("======================================================");

                for e in orderbook.bid.iter().rev().take(20) {
                    println!("[{}] BID {:>20.6} {:>20.6} {}", symbol, e.0.as_f64(), (e.1).0.as_f64(), ts - (e.1).1 as i64);
                }

                rr
            }).forward(sink)
        });
    core.run(runner).unwrap();
}



