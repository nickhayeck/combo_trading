use ftx_us_derivs::order::Order;
use ftx_us_derivs::table::ContractSpecTable;
use ftx_us_derivs::ws::{WebSocketMsg};
use binance::websockets::WebsocketEvent;
use binance::model::BookTickerEvent;
use std::cell::Ref;
use std::str::FromStr;
use std::collections::HashMap;

use crate::options_chain::{LedgerXOptionsChain, LedgerXOptionsContract, LatticeRef};

pub struct ComboStrat {
    opts_chain: LedgerXOptionsChain,
    spec_table: ContractSpecTable,
    last_spot_tick: Option<BookTickerEvent>,
    pub config: ComboStratConfig,
}

pub struct ComboStratConfig {
    pub symbol: String,
    pub ann_borrow_rate: f64,
    pub opts_tc: f64,
    pub spot_tc: f64,
}

// move the trade objects to some other file at some point I reckon
#[derive(Debug)]
pub struct BinanceMarketOrder {
    pub symbol: String,
    pub is_buy: bool,
    pub qty: f64,
    pub price: f64,
}

#[derive(Debug)]
pub struct Trade {
    pub binance: Vec<BinanceMarketOrder>,
    pub ledgerx: Vec<Order>,
}
impl Trade {
    pub fn empty() -> Self {
        Trade {
            binance: vec![],
            ledgerx: vec![],
        }
    }
    pub fn net_out(&mut self) {
        // only leave in the first order corresponding to each symbol
        // for each order that repeats a symbol, update quantity and is_buy of original
        let mut first_orders: HashMap<&str, BinanceMarketOrder> = HashMap::new();

        for curr in self.binance.iter() {
            let dqty = curr.qty * (curr.is_buy as i8 * 2 - 1) as f64;
            first_orders.entry(&curr.symbol)
                .and_modify(|order| {
                    let qty = order.qty * (order.is_buy as i8 * 2 - 1) as f64;
                    order.qty = f64::abs(qty + dqty);
                    order.is_buy = (qty + dqty) > 0.0;
                })
                .or_insert(
                    BinanceMarketOrder {
                        symbol: curr.symbol.to_owned(),
                        is_buy: curr.is_buy,
                        qty: curr.qty,
                        price: curr.price,
                    }
                );
        }
        self.binance = first_orders.into_iter().filter_map(
            |(_symbol,order)| {
                if order.qty != 0.0 {
                    return Some(order);
                }
                None
            }).collect();
    }
    pub fn reversal(&mut self, cfg: &ComboStratConfig, spot: &BookTickerEvent, call: &LedgerXOptionsContract, put: &LedgerXOptionsContract) -> bool {
        let spot_bid_quantity: f64 = f64::from_str(&spot.best_bid_qty).unwrap();
        
        let min_size = (call.ask_quantity.unwrap() as f64 * call.spec.multiplier)
                        .min(put.bid_quantity.unwrap() as f64 * put.spec.multiplier)
                        .min(spot_bid_quantity);
        // can change sizing later; not sure how scared we are about (not) getting filled
        let trade_size_factor: f64 = 0.5;
        let trade_size = (min_size as f64) * trade_size_factor;
        if trade_size * call.spec.multiplier < 1.0 ||  trade_size * put.spec.multiplier < 1.0 { return false;}

        let binance_order = BinanceMarketOrder {
            symbol: String::from_str(&cfg.symbol).unwrap(),
            is_buy: false,
            qty: trade_size,
            price: f64::from_str(&spot.best_bid).unwrap(),
        };

        let call_order = Order::new(
            call.id,
            false,
            call.ask.unwrap(),
            (trade_size * call.spec.multiplier) as u64,
        );

        let put_order = Order::new(
            put.id,
            true,
            put.bid.unwrap(),
            (trade_size * put.spec.multiplier) as u64,
        );
        // Add to trade list
        self.binance.push(binance_order);
        
        self.ledgerx.push(call_order);
        self.ledgerx.push(put_order);

        return true;
    }
    pub fn conversion(&mut self, cfg: &ComboStratConfig, spot: &BookTickerEvent, call: &LedgerXOptionsContract, put: &LedgerXOptionsContract) -> bool {
        let spot_ask_quantity: f64 = f64::from_str(&spot.best_ask_qty).unwrap();


        let min_size = call.bid_quantity.unwrap()
                        .min(put.ask_quantity.unwrap())
                        .min(spot_ask_quantity);
        // can change sizing later; not sure how scared we are about (not) getting filled
        let trade_size_factor: f64 = 0.5;
        let trade_size = (min_size as f64) * trade_size_factor;
        if trade_size * call.spec.multiplier < 1.0 ||  trade_size * put.spec.multiplier < 1.0 { return false; }

        let binance_order = BinanceMarketOrder {
            symbol: String::from_str(&cfg.symbol).unwrap(),
            is_buy: true,
            qty: trade_size,
            price: f64::from_str(&spot.best_ask).unwrap(),
        };

        let call_order = Order::new(
            call.id,
            true,
            call.bid.unwrap(),
            (trade_size * call.spec.multiplier) as u64,
        );

        let put_order = Order::new(
            put.id,
            false,
            put.ask.unwrap(),
            (trade_size * put.spec.multiplier) as u64,
        );

        // Add to trade list
        self.binance.push(binance_order);
        
        self.ledgerx.push(call_order);
        self.ledgerx.push(put_order);
        return true;
    }
}

impl ComboStrat {
    pub fn startup(config: ComboStratConfig) -> Self {
        let spec_table = ContractSpecTable::build().expect("Failed to Build Contract Table!");
        Self {
            spec_table: spec_table.clone(),
            opts_chain: LedgerXOptionsChain::from_spec_table("CBTC",spec_table),
            last_spot_tick: None,
            config,
        }
    }
    fn is_rev_arb(&self, spot_bid: &str, call_ask: f64, put_bid: f64, strike: u64, tte: f64) -> bool {
        let spot_bid: f64 = f64::from_str(spot_bid).unwrap();
        let synth_long = call_ask - put_bid + (strike as f64) * (-self.config.ann_borrow_rate * tte).exp();
        
        return 2.0*(spot_bid - synth_long) / (spot_bid + synth_long) > (self.config.opts_tc + self.config.spot_tc);
    }
    fn is_conv_arb(&self, spot_ask: &str, call_bid: f64, put_ask: f64, strike: u64, tte: f64) -> bool {
        let spot_ask: f64 = f64::from_str(spot_ask).unwrap();
        let synth_short = call_bid - put_ask + (strike as f64) * (-self.config.ann_borrow_rate * tte).exp();
        
        return 2.0*(synth_short - spot_ask) / (synth_short + spot_ask) > (self.config.opts_tc + self.config.spot_tc);
    }
    pub fn process_spot_update(&mut self, msg: WebsocketEvent) -> Option<Trade> {
        let mut out = Trade::empty();
        let mut ret_flag = false;
        if let WebsocketEvent::BookTicker(spot_bt) = msg {
            debug_assert!(self.config.symbol == spot_bt.symbol);

            for call_ext in self.opts_chain.calls.iter() {
                let call = call_ext.as_ref().borrow();
                
                if let Some(put_ext) = &call.adjacent {
                    let put = put_ext.lattice_deref();
                    let put = put.as_ref().borrow();

                    if self.arb_check(&mut out, call, put, &spot_bt) {
                        ret_flag = true;
                    }
                }
            }

            self.last_spot_tick = Some(spot_bt);
        }

        if ret_flag {
            out.net_out();
            return Some(out);
        } else {
            return None;
        }
    }

    pub fn process_opts_update(&self, msg: WebSocketMsg) -> Option<Trade> {
        let mut out = Trade::empty();
        if let WebSocketMsg::BookTop(new_bt) = msg {
            // check that we're looking at an options contract
            let contr = self.spec_table.id_table.get(&new_bt.contract_id).expect("Unseen Contract!");
            if contr.as_opt_ref().is_some() {
                let option_ref = self.opts_chain.id_map.get(&new_bt.contract_id)?;
                let mut option = option_ref.as_ref().borrow_mut();

                // update the chain
                option.bid = Some(new_bt.bid);
                option.bid_quantity = Some(new_bt.bid_size as f64);
                option.ask = Some(new_bt.ask);
                option.ask_quantity = Some(new_bt.ask_size as f64);
                drop(option);

                let option = option_ref.as_ref().borrow();

                // if we have data on the adjacent option
                let adj_option_ref = option.adjacent.to_owned()?;
                let adj_option_inner = adj_option_ref.upgrade().expect("what the fuck");
                let adj_option = adj_option_inner.as_ref().borrow();

                // and if we have spot data, check out the possibility of arbs on this level
                let spot_tick = self.last_spot_tick.as_ref()?;

                let (call,put) = if option.is_call { (option,adj_option) } else { (adj_option,option) }; 

                if self.arb_check(&mut out, call, put, spot_tick) {
                    out.net_out();
                    return Some(out);
                }
            }
        }
        return None;
    }

    fn arb_check(&self, out: &mut Trade, call: Ref<LedgerXOptionsContract>, put: Ref<LedgerXOptionsContract>, spot_tick: &BookTickerEvent) -> bool {
        
        let conv = match (call.bid, put.ask) {
            (Some(bid), Some(ask)) => self.is_conv_arb(&spot_tick.best_ask, bid, ask, call.strike, call.tte),
            _ => false,
        };

        let rev = match (call.ask, put.bid) {
            (Some(ask), Some(bid)) => self.is_rev_arb(&spot_tick.best_bid, ask, bid, call.strike, call.tte),
            _ => false,
        };

        debug_assert!(!(conv && rev));
        
        if conv {
            return out.conversion(&self.config, &spot_tick, &call, &put);
        }
        if rev {
            return out.reversal(&self.config, &spot_tick, &call, &put);
        }

        false
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::rc::Rc;

    use binance::model::BookTickerEvent;
    use chrono::{Utc, DateTime};
    use ftx_us_derivs::table::{ContractSpecTable, OptionContractSpec, ContractSpec};
    use ftx_us_derivs::ws::{WebSocketMsg, BookTop};

    use crate::{UniversalMsgWrapper, do_trade};
    use crate::options_chain::LedgerXOptionsChain;

    use super::{ComboStrat, ComboStratConfig, Trade, BinanceMarketOrder};

    fn mock_contract_table() -> ContractSpecTable {
        let call = OptionContractSpec {
            id: 22248027,
            label: "BTC-Mini-30JUN2023-10000-Call".to_string(),
            underlying: "CBTC".to_string(),
            strike_price: 10000,
            is_call: true,
            tte: 0.6600180575256285,
            open_interest: 207,
            multiplier: 100.0,
            min_increment: 1.0,
            active: true,
            date_live: DateTime::<Utc>::MIN_UTC, // unused for strat test
            date_expires: DateTime::<Utc>::MIN_UTC, // unused for strat test
            collateral_asset: "CBTC".to_string(),
            is_ecp_only: false,
        };

        let put = OptionContractSpec {
            id: 22248028,
            label: "BTC-Mini-30JUN2023-10000-Put".to_string(),
            underlying: "CBTC".to_string(),
            strike_price: 10000,
            is_call: false,
            tte: 0.6600180575256285,
            open_interest: 42,
            multiplier: 100.0,
            min_increment: 1.0,
            active: true,
            date_live: DateTime::<Utc>::MIN_UTC, // unused for strat test
            date_expires: DateTime::<Utc>::MIN_UTC, // unused for strat test
            collateral_asset: "USD".to_string(),
            is_ecp_only: false,
        };

        ContractSpecTable {
            id_table: HashMap::from_iter([
                (22248027, Rc::new(ContractSpec::Option(call))),
                (22248028, Rc::new(ContractSpec::Option(put)))
            ]),
            label_table: HashMap::new(),
        }
    }

    fn mock_msg_stream() -> Vec<UniversalMsgWrapper> {
        let symbol = "BTCUSDT".to_string();
        let qty = "1.0".to_string();
        vec![
            // initial spot update
            UniversalMsgWrapper::Binance(binance::websockets::WebsocketEvent::BookTicker(
                BookTickerEvent { update_id: 0, symbol: symbol.clone(), best_bid: "20449.0".to_string(), best_bid_qty: qty.clone(), best_ask: "20450.0".to_string(), best_ask_qty: qty.clone() }
            )), 
            // call update
            UniversalMsgWrapper::LedgerX(WebSocketMsg::BookTop(
                BookTop { bid: 11070.0, bid_size: 1, ask: 11180.0, ask_size: 1, contract_id: 22248027, contract_type: 0, clock: 0 }
            )),
            // put update
            UniversalMsgWrapper::LedgerX(WebSocketMsg::BookTop(
                BookTop { bid: 500.0, bid_size: 1, ask: 580.0, ask_size: 1, contract_id: 22248028, contract_type: 0, clock: 0 }
            )),
            // arbitrage-able spot update
            UniversalMsgWrapper::Binance(binance::websockets::WebsocketEvent::BookTicker(
                BookTickerEvent { update_id: 0, symbol, best_bid: "20299.0".to_string(), best_bid_qty: qty.clone(), best_ask: "20300.0".to_string(), best_ask_qty: qty.clone() }
            )),
        ]
    }


    #[test]
    fn find_arb() {
        let table = mock_contract_table();
        let chain = LedgerXOptionsChain::from_spec_table("CBTC", table.to_owned());
        let cfg = ComboStratConfig {
            symbol: "BTCUSDT".to_string(),
            ann_borrow_rate: 0.02,
            opts_tc: 0.0,
            spot_tc: 0.0,
        };

        let mut strat = ComboStrat {
            opts_chain: chain,
            spec_table: table.to_owned(),
            last_spot_tick: None,
            config: cfg,
        };
        for msg in mock_msg_stream() {
            let out = match dbg!(msg) {
                UniversalMsgWrapper::Binance(bn) => strat.process_spot_update(bn),
                UniversalMsgWrapper::LedgerX(lx) => strat.process_opts_update(lx),
            };
            if let Some(t) = out {
                assert!(do_trade(t));
            }
        }
    }

    #[test]
    fn test_net_out() {
        let mut trade = Trade {
            binance : vec![
                BinanceMarketOrder {
                    symbol: "coin".to_string(),
                    is_buy: true,
                    qty: 60.0,
                    price: 0.0,
                },
                BinanceMarketOrder {
                    symbol: "quoyn".to_string(),
                    is_buy: true,
                    qty: 80.0,
                    price: 0.0,
                },
                BinanceMarketOrder {
                    symbol: "coin".to_string(),
                    is_buy: false,
                    qty: 200.0,
                    price: 0.0,
                },
                BinanceMarketOrder {
                    symbol: "quoyn".to_string(),
                    is_buy: false,
                    qty: 60.0,
                    price: 0.0,
                },
                BinanceMarketOrder {
                    symbol: "coin".to_string(),
                    is_buy: true,
                    qty: 70.0,
                    price: 0.0,
                },
                BinanceMarketOrder {
                    symbol: "koheen".to_string(),
                    is_buy: true,
                    qty: 100.0,
                    price: 0.0,
                },
                BinanceMarketOrder {
                    symbol: "koheen".to_string(),
                    is_buy: false,
                    qty: 100.0,
                    price: 0.0,
                },
            ],
            ledgerx: vec![],
        };
        trade.net_out();
        println!("{:?}", trade)
    }
}