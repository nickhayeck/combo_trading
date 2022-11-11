use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Sender, SendError};
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use binance::errors::{ErrorKind, Error};
use ftx_us_derivs::error::WebSocketError;
use ftx_us_derivs::ws::WebSocketMsg;
use ftx_us_derivs::ws::WebSocketClient;
use ftx_us_derivs::order::OrderMngr;

use binance::websockets::{WebsocketEvent, WebSockets};
use binance::api::Binance;
use binance::account::Account;
use strat::{Trade, ComboStratConfig};




pub mod options_chain;
pub mod strat;

const LEDGERX_BASE_URL: &str = "";
const LEDGERX_WSS_URL: &str = "wss://api.ledgerx.com/ws";
const LEDGERX_API_KEY: &str = "";

const BINANCE_API_KEY: &str = "";
const BINANCE_API_SECRET: &str = "";

#[derive(Debug)]
pub enum UniversalMsgWrapper {
    Binance(WebsocketEvent),
    LedgerX(WebSocketMsg),
}

#[derive(Debug)]
pub enum UniversalErrorWrapper {
    ChannelError(SendError<UniversalMsgWrapper>),
    LedgerXWS(WebSocketError),
    BinanceError(Error),
}


fn ledgerx_msg_generator(tx: Sender<UniversalMsgWrapper>, run_flag: Arc<AtomicBool>) -> Result<(), UniversalErrorWrapper> {
    println!("Starting LedgerX Message Generator...");

    let mut client = WebSocketClient::connect(LEDGERX_WSS_URL)
        .map_err(|x|{UniversalErrorWrapper::LedgerXWS(x)})?;

    while run_flag.load(Ordering::Relaxed) {
        match client.yield_msg() {
            Ok(msg) => {
                tx.send(UniversalMsgWrapper::LedgerX(msg))
                    .map_err(|x| { UniversalErrorWrapper::ChannelError(x) })?;
            },
            Err(err) => {
                eprintln!("Error Encountered in LedgerX Message Parsing: {:?}", err);
            },
        }
    }    

    println!("Killing LedgerX Message Generator...");
    Ok(())
}

fn binance_msg_generator(tx: Sender<UniversalMsgWrapper>, run_flag: Arc<AtomicBool>) -> Result<(), UniversalErrorWrapper> {
    println!("Starting Binance Message Generator...");

    let mut client = WebSockets::new(move|msg| {
        tx.send(UniversalMsgWrapper::Binance(msg))
            .map_err(|x| Error::from_kind(ErrorKind::Msg(x.to_string())))
        }
    );


    client.connect("btcusdt@bookTicker").unwrap();

    client.event_loop(&*run_flag)
        .map_err(|x| UniversalErrorWrapper::BinanceError(x))?;
    
    println!("Stopping Binance Message Generator...");
    Ok(())
}

fn start_msg_channels(
    tx: &Sender<UniversalMsgWrapper>, 
    run_flag: &Arc<AtomicBool>
) -> (JoinHandle<Result<(), UniversalErrorWrapper>>, JoinHandle<Result<(), UniversalErrorWrapper>>) {
    let lx_tx = tx.clone();
    let lx_flg = run_flag.clone();
    let thread_handler_ledgerx = std::thread::spawn(move|| {
        ledgerx_msg_generator(lx_tx, lx_flg)
    });

    let bn_tx = tx.clone();
    let bn_flg = run_flag.clone();
    let thread_handler_binance = std::thread::spawn(move|| {
        binance_msg_generator(bn_tx, bn_flg)
    });

    
    let run_flag_handle = run_flag.clone();
    ctrlc::set_handler(move || {
        run_flag_handle.store(false, Ordering::Relaxed);
    }).expect("Error setting Ctrl-C handler");

    return (thread_handler_ledgerx, thread_handler_binance);
}

fn do_trade(t: Trade) -> bool {
    let mut msg = String::new();

    let binance_res: Vec<_> = t.binance.into_iter().map(|binance_order| {
        // if binance_order.is_buy {
        //     binance_om.market_buy(binance_order.symbol, binance_order.qty)
        // } else {
        //     binance_om.market_sell(binance_order.symbol, binance_order.qty)
        // }
        msg.push_str(format!(
            "{} {}: {}x{};",
            binance_order.symbol,
            if binance_order.is_buy {"bid"} else {"ask"},
            binance_order.price,
            binance_order.qty,
        ).as_str());
    }).collect();

    let ledgerx_res: Vec<_> = t.ledgerx.into_iter().map(|ledgerx_order| {
        //ledgerx_om.send_order(&ledgerx_order)
        msg.push_str(&format!(
            "id_{} {}: {}x{};",
            ledgerx_order.contract_id, // can call or put be inferred from contract id?
            if ledgerx_order.is_ask {"ask"} else {"bid"},
            ledgerx_order.price,
            ledgerx_order.size,
        ))
    }).collect();

    println!("{msg}");

    true
}

fn main() {

    // connections to the exchanges
    let mut ledgerx_om = OrderMngr::new(LEDGERX_BASE_URL, LEDGERX_API_KEY);
    let binance_om: Account = Binance::new(Some(BINANCE_API_KEY.to_string()), Some(BINANCE_API_SECRET.to_string()));
    
    // strategy configuration and startup
    let strat_config = ComboStratConfig {
        symbol: String::from_str("BTCUSDT").unwrap(),
        ann_borrow_rate: 0.03,
        opts_tc: 10e-4,
        spot_tc: 10e-4,
    };
    let mut strat = strat::ComboStrat::startup(strat_config);

    // interprocess/thread communication
    let (tx, rx) = channel::<UniversalMsgWrapper>();
    let run_flag = Arc::new(AtomicBool::new(true));
    let (lx_handle, bn_handle) = start_msg_channels(&tx, &run_flag);

    // event processing loop
    while run_flag.load(Ordering::Relaxed) {
        let msg = rx.recv().unwrap();
        // println!("{:?}", msg);

        let trade = match msg {
            UniversalMsgWrapper::Binance(spot) => strat.process_spot_update(spot),
            UniversalMsgWrapper::LedgerX(opts) => strat.process_opts_update(opts),
        };


        if let Some(t) = trade {
            assert!(do_trade(t));
        }
    }

    // Cleanup
    if let Err(e) = lx_handle.join() {
        eprintln!("{:?}", e);
    }
    if let Err(e) = bn_handle.join() {
        eprintln!("{:?}", e);
    }
}
