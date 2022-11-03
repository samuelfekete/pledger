use std::error::Error;
use std::pin::Pin;
use std::str::FromStr;

use futures_core::stream::Stream;
use sqlx::sqlite::SqliteJournalMode;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePool;

#[derive(sqlx::FromRow, Debug, Eq, Hash, PartialEq)]
pub struct ClientID {
    pub client_id: u16,
}

#[derive(sqlx::FromRow, Debug, Eq, Hash, PartialEq)]
pub struct MutableTransaction {
    pub ordinal: i64,
    pub client_id: u16,
    pub transaction_id: u32,
    pub amount: String,
    pub disputed: bool,
    pub charged_back: bool,
}

#[derive(Clone)]
pub struct MutableTransactionStore {
    db_pool: SqlitePool
}

impl MutableTransactionStore {
    pub async fn new(url: &str) -> Result<Self, Box<dyn Error>> {
        let db_pool = SqlitePool::connect_with(
            SqliteConnectOptions::from_str(url)?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true)
        ).await?;

        Ok(Self{ db_pool })
    }

    pub async fn clean_and_recreate(&self) -> Result<(), Box<dyn Error>> {
        sqlx::query("
            DROP TABLE IF EXISTS transactions;

            CREATE TABLE transactions
            (
                ordinal         INTEGER PRIMARY KEY,
                client_id       INTEGER,
                transaction_id  INTEGER UNIQUE,
                amount          TEXT,
                disputed        BOOLEAN,
                charged_back    BOOLEAN
            );
        ").execute(&self.db_pool).await?;

        Ok(())
    }

    pub async fn insert_transaction(&self, client_id: u16, transaction_id: u32, amount: &str) -> Result<(), Box<dyn Error>> {
        sqlx::query("
            INSERT INTO transactions (
                client_id, transaction_id, amount, disputed, charged_back
            ) VALUES ($1, $2, $3, false, false)
            ON CONFLICT (transaction_id) DO NOTHING;
        ")
        .bind(client_id)
        .bind(transaction_id)
        .bind(amount)
        .execute(&self.db_pool).await?;
    
        Ok(())
    }

    pub async fn dispute_transaction(&self, client_id: u16, transaction_id: u32) -> Result<(), Box<dyn Error>> {
        sqlx::query("
                UPDATE transactions 
                SET disputed = true
                WHERE client_id = $1 AND transaction_id = $2;
        ")
        .bind(client_id)
        .bind(transaction_id)
        .execute(&self.db_pool).await?;

        Ok(())
    }

    pub async fn resolve_dispute(&self, client_id: u16, transaction_id: u32) -> Result<(), Box<dyn Error>> {
        sqlx::query("
                UPDATE transactions 
                SET disputed = false
                WHERE client_id = $1 AND transaction_id = $2;
        ")
        .bind(client_id)
        .bind(transaction_id)
        .execute(&self.db_pool).await?;

        Ok(())
    }

    pub async fn chargeback_transaction(&self, client_id: u16, transaction_id: u32) -> Result<(), Box<dyn Error>> {
        sqlx::query("
                UPDATE transactions 
                SET disputed = false, charged_back = true
                WHERE client_id = $1 AND transaction_id = $2 AND disputed = true;
        ")
        .bind(client_id)
        .bind(transaction_id)
        .execute(&self.db_pool).await?;

        Ok(())
    }

    pub async fn get_clients(&self) -> Pin<Box<dyn Stream<Item = Result<ClientID, sqlx::Error>> + Send + '_>> {
        sqlx::query_as::<_, ClientID>("
            SELECT DISTINCT client_id from transactions;
        ")
        .fetch(&self.db_pool)
    }

    pub async fn get_transactions_for_client(&self, client_id: u16) -> Pin<Box<dyn Stream<Item = Result<MutableTransaction, sqlx::Error>> + Send + '_>> {
        sqlx::query_as::<_, MutableTransaction>("
            SELECT * from transactions
            WHERE client_id = $1
            ORDER BY ordinal;
        ")
        .bind(client_id)
        .fetch(&self.db_pool)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::iter::FromIterator;
    use futures_util::TryStreamExt;

    #[tokio::test]
    async fn test_insert_transactions() {
        let store = MutableTransactionStore::new("sqlite::memory:").await.unwrap();
        store.clean_and_recreate().await.unwrap();

        store.insert_transaction(7, 15, "2.50").await.unwrap();
        store.insert_transaction(7, 19, "3.50").await.unwrap();

        let transactions: Vec<MutableTransaction> = store.get_transactions_for_client(7).await.try_collect().await.unwrap();
        let expected: Vec<MutableTransaction> = vec![
            MutableTransaction {
                ordinal: 1,
                client_id: 7,
                transaction_id: 15,
                amount: "2.50".into(),
                disputed: false,
                charged_back: false,
            }, 
            MutableTransaction {
                ordinal: 2,
                client_id: 7,
                transaction_id: 19,
                amount: "3.50".into(),
                disputed: false,
                charged_back: false,
            },
        ];

        assert_eq!(transactions, expected);
    }

    #[tokio::test]
    async fn test_dispute_transactions() {
        let store = MutableTransactionStore::new("sqlite::memory:").await.unwrap();
        store.clean_and_recreate().await.unwrap();

        store.insert_transaction(7, 15, "2.50").await.unwrap();
        store.dispute_transaction(7, 15).await.unwrap();

        let transactions: Vec<MutableTransaction> = store.get_transactions_for_client(7).await.try_collect().await.unwrap();
        let expected: Vec<MutableTransaction> = vec![
            MutableTransaction {
                ordinal: 1,
                client_id: 7,
                transaction_id: 15,
                amount: "2.50".into(),
                disputed: true,
                charged_back: false,
            },
        ];

        assert_eq!(transactions, expected);
    }

    #[tokio::test]
    async fn test_resolve_dispute() {
        let store = MutableTransactionStore::new("sqlite::memory:").await.unwrap();
        store.clean_and_recreate().await.unwrap();

        store.insert_transaction(7, 15, "2.50").await.unwrap();
        store.dispute_transaction(7, 15).await.unwrap();
        store.resolve_dispute(7, 15).await.unwrap();

        let transactions: Vec<MutableTransaction> = store.get_transactions_for_client(7).await.try_collect().await.unwrap();
        let expected: Vec<MutableTransaction> = vec![
            MutableTransaction {
                ordinal: 1,
                client_id: 7,
                transaction_id: 15,
                amount: "2.50".into(),
                disputed: false,
                charged_back: false,
            },
        ];

        assert_eq!(transactions, expected);
    }

    #[tokio::test]
    async fn test_chargeback_transaction() {
        let store = MutableTransactionStore::new("sqlite::memory:").await.unwrap();
        store.clean_and_recreate().await.unwrap();

        store.insert_transaction(7, 15, "2.50").await.unwrap();
        store.dispute_transaction(7, 15).await.unwrap();
        store.chargeback_transaction(7, 15).await.unwrap();

        let transactions: Vec<MutableTransaction> = store.get_transactions_for_client(7).await.try_collect().await.unwrap();
        let expected: Vec<MutableTransaction> = vec![
            MutableTransaction {
                ordinal: 1,
                client_id: 7,
                transaction_id: 15,
                amount: "2.50".into(),
                disputed: false,
                charged_back: true,
            },
        ];

        assert_eq!(transactions, expected);
    }

    #[tokio::test]
    async fn test_get_clients() {
        let store = MutableTransactionStore::new("sqlite::memory:").await.unwrap();
        store.clean_and_recreate().await.unwrap();

        store.insert_transaction(7, 15, "2.50").await.unwrap();
        store.insert_transaction(8, 13, "2.50").await.unwrap();
        store.insert_transaction(7, 19, "2.50").await.unwrap();

        let clients: HashSet<ClientID> = store.get_clients().await.try_collect().await.unwrap();
        let expected: HashSet<ClientID> = HashSet::from_iter(vec![ClientID { client_id: 7}, ClientID { client_id: 8}]);

        assert_eq!(clients, expected);
    }

    #[tokio::test]
    async fn test_get_transactions_for_client() {
        let store = MutableTransactionStore::new("sqlite::memory:").await.unwrap();
        store.clean_and_recreate().await.unwrap();

        store.insert_transaction(7, 15, "2.50").await.unwrap();
        store.insert_transaction(8, 13, "2.50").await.unwrap();
        store.insert_transaction(7, 19, "3.50").await.unwrap();

        let transactions: Vec<MutableTransaction> = store.get_transactions_for_client(7).await.try_collect().await.unwrap();
        let expected: Vec<MutableTransaction> = vec![
            MutableTransaction {
                ordinal: 1,
                client_id: 7,
                transaction_id: 15,
                amount: "2.50".into(),
                disputed: false,
                charged_back: false,
            }, 
            MutableTransaction {
                ordinal: 3,
                client_id: 7,
                transaction_id: 19,
                amount: "3.50".into(),
                disputed: false,
                charged_back: false,
            },
        ];

        assert_eq!(transactions, expected);
    }

}
