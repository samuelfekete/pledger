use std::error::Error;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use serde::Serialize;


#[derive(Debug, Serialize, Hash, PartialEq, Eq)]
pub struct OutputAccount {
    pub client: u16,
    pub available: BigDecimal,
    pub held: BigDecimal,
    pub total: BigDecimal,
    pub locked: bool,
}

impl OutputAccount {
    pub fn new(client: u16, available: &str, held: &str, total: &str, locked: bool) -> Result<Self, Box<dyn Error>> {
        Ok(OutputAccount {
            client,
            available: BigDecimal::from_str(available)?,
            held: BigDecimal::from_str(held)?,
            total: BigDecimal::from_str(total)?,
            locked,
        })
    }

    pub fn round_amounts(&mut self, round_digits: i64) {
        self.available = self.available.round(round_digits);
        self.held = self.held.round(round_digits);
        self.total = self.total.round(round_digits);
    }
}

