use std::error::Error;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use serde::Deserialize;


#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")] 
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct InputTransaction {
    #[serde(alias = "type")]
    pub transaction_type: TransactionType,
    pub client: u16,
    pub tx: u32,
    pub amount: Option<BigDecimal>,
}

impl InputTransaction {
    pub fn new(transaction_type: TransactionType, client: u16, tx: u32, amount: Option<&str>) -> Result<Self, Box<dyn Error>> {
        let converted_amount = match amount { 
            None => None,
            Some(amount) => Some(BigDecimal::from_str(amount)?)
        };
        Ok(InputTransaction {
            transaction_type,
            client,
            tx,
            amount: converted_amount,
        })
    }
}

pub fn parse_input_transaction<R>(input_stream: R) -> csv::DeserializeRecordsIntoIter<R, InputTransaction>
where R: std::io::Read
{
    let reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(input_stream);
    reader.into_deserialize()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_input() {
        let input = "
            type, client, tx, amount
            deposit, 7, 11, 42.0
            withdrawal, 9, 18, 6.5
        ";
        let input_transactions: Vec<InputTransaction> = parse_input_transaction(input.as_bytes())
            .filter_map(|t| t.ok())
            .collect();
        let expected = vec![
            InputTransaction {
                transaction_type: TransactionType::Deposit, 
                client: 7,
                tx: 11,
                amount: Some(BigDecimal::from_str("42.0").unwrap())
            }, 
            InputTransaction {
                transaction_type: TransactionType::Withdrawal, 
                client: 9,
                tx: 18,
                amount: Some(BigDecimal::from_str("6.5").unwrap())
            }, 
        ];
        assert_eq!(input_transactions, expected)
    }

    #[test]
    fn test_empty_input() {
        let input = "";
        let input_transactions: Vec<InputTransaction> = parse_input_transaction(input.as_bytes())
            .filter_map(|t| t.ok())
            .collect();
        let expected = vec![];
        assert_eq!(input_transactions, expected)
    }
}
