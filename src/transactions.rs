use std::error::Error;
use std::str::FromStr;

use async_stream::try_stream;
use bigdecimal::{BigDecimal, Zero, Signed};
use futures_core::Stream;
use futures_util::stream::TryStreamExt;

use crate::input::{InputTransaction, TransactionType};
use crate::output::OutputAccount;
use crate::transactions_store::{MutableTransactionStore};

#[derive(Clone)]
pub struct Transactions {
    transactions_store: MutableTransactionStore,
}

impl Transactions {
    pub async fn new(db_url: &str) -> Result<Self, Box<dyn Error>> {
        let transactions_store = MutableTransactionStore::new(db_url).await?;
        transactions_store.clean_and_recreate().await?;
        Ok(Self{ transactions_store })
    }

    pub async fn add_input(&self, input_transaction: InputTransaction) -> Result<(), Box<dyn Error>> {
        match input_transaction.transaction_type {
            TransactionType::Deposit => {
                self.transactions_store.insert_transaction(
                    input_transaction.client,
                    input_transaction.tx,
                    &input_transaction.amount.ok_or("Deposit must have an amount")?.to_string(),
                ).await?
            },
            TransactionType::Withdrawal => {
                self.transactions_store.insert_transaction(
                    input_transaction.client,
                    input_transaction.tx,
                    &(-input_transaction.amount.ok_or("Deposit must have an amount")?).to_string(),
                ).await?
            },
            TransactionType::Dispute => {
                self.transactions_store.dispute_transaction(
                    input_transaction.client,
                    input_transaction.tx,
                ).await?
            },
            TransactionType::Resolve => {
                self.transactions_store.resolve_dispute(
                    input_transaction.client,
                    input_transaction.tx,
                ).await?
            },
            TransactionType::Chargeback => {
                self.transactions_store.chargeback_transaction(
                    input_transaction.client,
                    input_transaction.tx,
                ).await?
            },
        }
        Ok(())
    }

    pub async fn get_account_for_client(&self, client_id: u16) -> Result<OutputAccount, Box<dyn Error>> {
        let mut transactions = self.transactions_store.get_transactions_for_client(client_id).await;
        let mut account = OutputAccount{
            client: client_id,
            available: BigDecimal::zero(),
            held: BigDecimal::zero(),
            total: BigDecimal::zero(),
            locked: false,
        };
        while let Some(transaction) = transactions.try_next().await? {
            if transaction.charged_back {
                account.locked = true;
                return Ok(account)
            }
            let transaction_amount = BigDecimal::from_str(&transaction.amount)?;

            let mut new_held = account.held.clone();
            let mut new_available = account.available.clone();
            if transaction.disputed {
                new_held += transaction_amount.abs();
                if transaction_amount.is_negative() {
                    new_available += transaction_amount
                }
            } else {
                new_available += transaction_amount
            }
            let new_total = new_available.clone() + new_held.clone();
            if new_available < BigDecimal::zero() {
                continue;
            }

            account.available = new_available;
            account.held = new_held;
            account.total = new_total;
        }
        account.round_amounts(4);
        Ok(account)
    }

    pub async fn get_accounts(self) -> impl Stream<Item = Result<OutputAccount, Box<dyn Error>>> {
        try_stream! {
            let mut client_ids = self.transactions_store.get_clients().await;
            while let Some(row) = client_ids.try_next().await? {
                let client_id = row.client_id;

                let account = self.get_account_for_client(client_id).await?;
                yield account;
            }
        }
    }
}

 #[cfg(test)]
 mod tests {
    use super::*;

    use std::collections::HashSet;

    async fn run_test_scenario(transactions: Vec<InputTransaction>, expected_accounts: HashSet<OutputAccount>) {
        let engine = Transactions::new("sqlite::memory:").await.unwrap();
        for transaction in transactions {
            engine.add_input(transaction).await.unwrap();
        }
        let actual_accounts: HashSet<OutputAccount> = engine.get_accounts().await.try_collect().await.unwrap();
        assert_eq!(actual_accounts, expected_accounts)
    }

    #[tokio::test]
    async fn test_deposit_and_withdrawal_one_client() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("50")).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "50", "0", "50", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_withdrawal_more_than_deposit() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("200")).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "100", "0", "100", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_transaction_in_dispute() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  1, None).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "0", "100", "100", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_disputed_deposit_followed_by_withdrawal() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("50")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  1, None).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "0", "100", "100", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_disputed_invalid_withdrawal() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("200")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  2, None).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "100", "0", "100", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_disputed_withdrawal_followed_by_withdrawal() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("50")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  2, None).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  3, Some("100")).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "50", "50", "100", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_disputed_valid_withdrawal() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("50")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  2, None).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "50", "50", "100", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_resolve_dispute() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  1, None).unwrap(),
                InputTransaction::new(TransactionType::Resolve,     1,  1, None).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "100", "0", "100", false).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_chargeback_on_deposit() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Deposit,     1,  2, Some("50")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  2, None).unwrap(),
                InputTransaction::new(TransactionType::Deposit,     1,  3, Some("30")).unwrap(),
                InputTransaction::new(TransactionType::Chargeback,  1,  2, None).unwrap(),
                InputTransaction::new(TransactionType::Deposit,     1,  4, Some("25")).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "100", "0", "100", true).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_chargeback_on_withdrawal() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("50")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  2, None).unwrap(),
                InputTransaction::new(TransactionType::Deposit,     1,  3, Some("30")).unwrap(),
                InputTransaction::new(TransactionType::Chargeback,  1,  2, None).unwrap(),
                InputTransaction::new(TransactionType::Deposit,     1,  4, Some("25")).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "100", "0", "100", true).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_chargeback_on_invalid_withdrawal() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("200")).unwrap(),
                InputTransaction::new(TransactionType::Dispute,     1,  2, None).unwrap(),
                InputTransaction::new(TransactionType::Chargeback,  1,  2, None).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "100", "0", "100", true).unwrap(),
            ])
        ).await;
    }

    #[tokio::test]
    async fn test_chargeback_on_transaction_not_in_dispute() {
        run_test_scenario(
            vec![
                InputTransaction::new(TransactionType::Deposit,     1,  1, Some("100")).unwrap(),
                InputTransaction::new(TransactionType::Withdrawal,  1,  2, Some("50")).unwrap(),
                InputTransaction::new(TransactionType::Chargeback,  1,  2, None).unwrap(),
            ], 
            HashSet::from([
                OutputAccount::new(1, "50", "0", "50", false).unwrap(),
            ])
        ).await;
    }
 }