use std::error::Error;
use std::io;
use std::io::{Read, Write};

use futures_util::pin_mut;
use futures_util::TryStreamExt;

pub mod input;
pub mod output;
pub mod transactions;
pub mod transactions_store;

// Get the input CSV as a Reader.
async fn get_input() -> Result<std::io::BufReader<std::fs::File>, Box<dyn Error>> {
    let filename = std::env::args().skip(1).next()
        .ok_or("A valid file name is required as an argument.")?;
    let file = std::fs::File::open(filename)?;
    let input_reader = std::io::BufReader::new(file);
    Ok(input_reader)
}

// Main transaction processor.
// Converts a CSV of transactions from `input` and writes a CSV of accounts to `output`. 
async fn process_transactions<R: Read, W: Write>(input: R, output: W, db_url: &str) -> Result<(), Box<dyn Error>> {
    let input_transactions = input::parse_input_transaction(input);

    let transactions = transactions::Transactions::new(db_url).await?;
    for result in input_transactions {
        let input_transaction = result?;
        transactions.add_input(input_transaction).await?;
    } 

    let accounts = transactions.get_accounts().await;

    let mut writer = csv::Writer::from_writer(output);

    pin_mut!(accounts);
    while let Some(account) = accounts.try_next().await? {
        writer.serialize(account)?;
    }
    writer.flush()?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    process_transactions(
        get_input().await?, 
        io::stdout(), 
        "sqlite://transactions.db"
    ).await
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_process_transactions() {
        let input = "
            type,       client, tx, amount
            deposit,    7,      1,  10.0
            withdrawal, 7,      2,  5.0";
        let expected_output = "client,available,held,total,locked\n7,5.0000,0,5.0000,false\n";
        let mut output = Vec::new();
        process_transactions(input.as_bytes(), &mut output, "sqlite::memory:").await.unwrap();
        
        let actual = String::from_utf8(output).unwrap();
        println!("{}", actual);
        assert_eq!(actual, expected_output)
    }
}
