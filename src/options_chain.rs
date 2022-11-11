use std::cell::{RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use ftx_us_derivs::table::{OptionContractSpec, ContractSpecTable};

use chrono::{offset::Utc, DateTime};


type LatticePointer = Weak<RefCell<LedgerXOptionsContract>>;

pub trait LatticeRef {
    type Target;
    fn lattice_deref(&self) -> Rc<RefCell<Self::Target>>;
}
impl LatticeRef for LatticePointer {
    type Target = LedgerXOptionsContract;
    fn lattice_deref(&self) -> Rc<RefCell<Self::Target>> {
        return self.upgrade().unwrap();
    }   
}

#[derive(Debug)]
pub struct LedgerXOptionsContract {
    pub id: u64,
    pub label: String,
    
    pub underlying: String,
    pub strike: u64,
    pub is_call: bool,
    pub tte: f64, // annualized

    pub bid: Option<f64>,
    pub bid_quantity: Option<f64>,
    pub ask: Option<f64>,
    pub ask_quantity: Option<f64>,
    
    pub adjacent: Option<LatticePointer>,
    pub up: Option<LatticePointer>,
    pub down: Option<LatticePointer>,

    pub spec: OptionContractSpec,

    //book: LedgerXOrderbook,
}

impl LedgerXOptionsContract {
    pub fn from_spec(spec: &OptionContractSpec) -> Self {
        Self {
            id: spec.id,
            label: spec.label.clone(),
            
            underlying: spec.underlying.clone(),
            strike: spec.strike_price,
            is_call: spec.is_call,
            tte: spec.tte,
            
            bid: None,
            bid_quantity: None,
            ask: None,
            ask_quantity: None,
            
            adjacent: None,
            up: None,
            down: None,
            
            spec: spec.to_owned(),
        }
    }
}

pub struct LedgerXOptionsChain {
    // Lists of all contracts for mass-repricing events, e.g. spot move.
    pub calls: Vec<Rc<RefCell<LedgerXOptionsContract>>>,
    pub puts: Vec<Rc<RefCell<LedgerXOptionsContract>>>,

    // Lookup tables by unique identifier for individual contract updates, e.g. market tick
    pub id_map: HashMap<u64, Rc<RefCell<LedgerXOptionsContract>>>,
    pub label_map: HashMap<String, Rc<RefCell<LedgerXOptionsContract>>>,

    // Data structure for lookup of contracts by expiration, strike, then parity
    pub expirys: HashMap<DateTime<Utc>, LedgerXExpiryBlock>,
}

pub struct LedgerXExpiryBlock {
    expiration: DateTime<Utc>,
    strikes: Vec<u64>,
    strike_map: HashMap<u64, LedgerXOptionsLevel>,
}

impl LedgerXExpiryBlock {
    pub fn iter_down(&self) {
        todo!()
    }
}

pub struct LedgerXOptionsLevel {
    expiration: DateTime<Utc>,
    strike: u64,
    
    pub call: Option<Rc<RefCell<LedgerXOptionsContract>>>,
    pub put: Option<Rc<RefCell<LedgerXOptionsContract>>>,
}



impl LedgerXOptionsChain {
    pub fn build(symbol: &str) -> Self {
        let table: ContractSpecTable = ContractSpecTable::build().unwrap();
        Self::from_spec_table(symbol, table)
    }

    pub fn from_spec_table(symbol: &str, table: ContractSpecTable) -> Self {
        let mut calls: Vec<Rc<RefCell<LedgerXOptionsContract>>> = vec![];
        let mut puts: Vec<Rc<RefCell<LedgerXOptionsContract>>> = vec![];
        
        // build the list of all options
        for spec in table.id_table.values().into_iter() {
            if let Some(s) = spec.as_opt_ref() {
                let contr = Rc::new(RefCell::new(LedgerXOptionsContract::from_spec(s)));
                if contr.as_ref().borrow().spec.active && contr.as_ref().borrow().underlying.as_str() == symbol {
                    match contr.as_ref().borrow().is_call {
                        true => calls.push(contr.clone()),
                        false => puts.push(contr.clone()),
                    }
                }
            }
        }
        
        // build the unique ID maps
        let mut id_map: HashMap<u64, Rc<RefCell<LedgerXOptionsContract>>> = HashMap::new();
        let mut label_map: HashMap<String, Rc<RefCell<LedgerXOptionsContract>>> = HashMap::new();
        for option_ref in calls.iter().chain(puts.iter()) {
            id_map.insert(option_ref.as_ref().borrow().id, option_ref.clone());
            label_map.insert(option_ref.as_ref().borrow().label.to_owned(), option_ref.clone());
        }

        // build the full data structure by nesting the Entry API
        let mut expirys: HashMap<DateTime<Utc>, LedgerXExpiryBlock>  = HashMap::new();
        for option_ref in calls.iter().chain(puts.iter()) {
            let option = option_ref.as_ref().borrow();

            expirys.entry(option.spec.date_expires)
                .and_modify(|exp_block| {
                    exp_block.strike_map.entry(option.strike)
                        .and_modify(|level| {
                            if option.is_call {
                                level.call = Some(option_ref.to_owned());
                            } else {
                                level.put = Some(option_ref.to_owned());
                            }
                        })
                        .or_insert(
                            LedgerXOptionsLevel {
                                expiration: option.spec.date_expires,
                                strike: option.strike,
                                call: if option.is_call { Some(option_ref.to_owned()) } else { None },
                                put: if !option.is_call { Some(option_ref.to_owned()) } else { None },
                            }
                        );

                    if !exp_block.strikes.contains(&option.strike) {
                        exp_block.strikes.push(option.strike);
                        exp_block.strikes.sort();
                    }
                })
                .or_insert(
                    LedgerXExpiryBlock {
                        expiration: option.spec.date_expires,
                        strikes: vec![option.strike],
                        strike_map: HashMap::from_iter(
                            [(
                                option.strike,
                                LedgerXOptionsLevel {
                                    expiration: option.spec.date_expires,
                                    strike: option.strike,
                                    call: if option.is_call { Some(option_ref.to_owned()) } else { None },
                                    put: if !option.is_call { Some(option_ref.to_owned()) } else { None },
                                }
                            )]
                        ),
                    }
                );
                
        }

        // link the all the internal structures of the options:
        // left-right linkage
        for call_ext in calls.iter() {
            let mut call = call_ext.as_ref().borrow_mut();

            let block = expirys.get(&call.spec.date_expires).unwrap();
            let level = block.strike_map.get(&call.strike).unwrap();
            
            if let Some(put) = &level.put {
                call.adjacent = Some(Rc::downgrade(put));
                put.as_ref().borrow_mut().adjacent = Some(Rc::downgrade(call_ext));
            }
        }
        // up-down linkage
        for exp in expirys.values() {
            let mut last_above = (None, None);
            let mut last_below = (None, None);
            for i in 0..(exp.strikes.len()) {
                // one index that crawls up and one that crawls down
                let crawl_down_ind = i;
                let crawl_up_ind = exp.strikes.len() - i - 1;
                
                // crawling down the chain and setting the "up" pointers
                let level = exp.strike_map.get(&exp.strikes[crawl_down_ind]).unwrap();
                if let Some(call) = &level.call {
                    call.as_ref().borrow_mut().up = last_above.0;
                    last_above = (Some(Rc::downgrade(call)), last_above.1);
                }
                if let Some(put) = &level.put {
                    put.as_ref().borrow_mut().up = last_above.1;
                    last_above = (last_above.0, Some(Rc::downgrade(put)));
                }

                // crawling up the chain and setting the "down" pointers
                let level = exp.strike_map.get(&exp.strikes[crawl_up_ind]).unwrap();
                if let Some(call) = &level.call {
                    call.as_ref().borrow_mut().down = last_below.0;
                    last_below = (Some(Rc::downgrade(call)), last_below.1);
                }
                if let Some(put) = &level.put {
                    put.as_ref().borrow_mut().down = last_below.1;
                    last_below = (last_below.0, Some(Rc::downgrade(put)));
                }
            }
        }

        
        
        return LedgerXOptionsChain {
            calls,
            puts,
            id_map,
            label_map,
            expirys,
        };
    }

    pub fn get_opts_level(expiration: DateTime<Utc>, strike: u64) -> Option<LedgerXOptionsLevel> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::str::FromStr;

    use super::LedgerXOptionsChain;
    #[test]
    fn id_map() {
        let chain = LedgerXOptionsChain::build("CBTC");

        let id1 = 22252867;
        let id2 = 22252868;

        println!("{:?}", chain.id_map.get(&id1).unwrap());
        println!("{:?}", chain.id_map.get(&id2).unwrap());
    }
    #[test]
    fn crawl_up() {
        let chain = LedgerXOptionsChain::build("CBTC");
        
        let eg_label = String::from_str("BTC-Mini-30JUN2023-200000-Call").unwrap();
        let eg = chain.label_map.get(&eg_label).unwrap().to_owned();
        
        let mut eg_crawler = Some(Rc::downgrade(&eg));
        loop {
            if let Some(e) = eg_crawler {
                let e_borrow = e.upgrade().unwrap();
                let e_borrow = e_borrow.as_ref().borrow();
                println!("{}", e_borrow.label); 
                eg_crawler = e_borrow.up.to_owned();
            } else {
                break;
            }
        }
    }

    #[test]
    fn crawl_down() {
        let chain = LedgerXOptionsChain::build("CBTC");
        
        let eg_label = String::from_str("BTC-Mini-30JUN2023-10000-Put").unwrap();
        let eg = chain.label_map.get(&eg_label).unwrap().to_owned();
        
        let mut eg_crawler = Some(Rc::downgrade(&eg));
        loop {
            if let Some(e) = eg_crawler {
                let e_borrow = e.upgrade().unwrap();
                let e_borrow = e_borrow.as_ref().borrow();
                println!("{}", e_borrow.label); 
                eg_crawler = e_borrow.down.to_owned();
            } else {
                break;
            }
        }
    }
}
