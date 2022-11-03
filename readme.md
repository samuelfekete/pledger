Pledger
=======

A prototype transaction ledger written in Rust.

Usage
-----
Ensure you have the latest Rust installed.
```
rustup update
```

From the root of the project, run using Cargo, providing a csv file as the only argument:
```
cargo run -- transactions.csv
```

Unit tests
-----------
Tests can be run using:
```
cargo test
```
Most of the functionality is covered by unit tests for the happy paths. 
There are more scenario tests in the transactions module covering different cases.

Assumptions
-----------

- The filename argument is always provided and it is valid Unicode.
- The supplied file exists, is valid CSV, and the values are valid for the specified types.
- A dispute can be in relation to a withdrawal transaction or a deposit transaction.
- A withdrawal can only be made from funds that are not held.
- A dispute on a transaction that is already in dispute can be ignored.
- A dispute can be ignored if it's in relation to a transaction that has been ignored for being more than the balance.
- A dispute on a deposit prevents withdrawals that depend on that deposit, 
even if the dispute was received after the withdrawals (as opposed to saying that some disputes are no longer allowed if the money has been withdrawn).
- A dispute on a withdrawal prevents withdrawals that would cause the account to go overdrawn if the dispute is resolved (amount available cannot be negative).
- The transaction data is already stored somewhere else and we don't need to worry about information loss.
- A chargeback on a transaction that was ignored because it would cause the account to go overdrawn still freezes the account.
- A frozen account should ignore both deposit and withdrawal transactions that come after the transaction that was charged back (even if the chargeback event happened later).
- A transaction that has been charged back can no longer be disputed or resolved, and any future disputes or resolves can be ignored.
- A transaction with a transaction ID that already exists can be ignored.
- Accounts start with a balance of 0.
- Amounts should be rounded to 4 decimal places.
- The text in the input (columns, transaction type) is always lowercase.

Design
------

Since disputes can refer to any previous transaction and do not include the amount, 
we have to keep a complete history of deposits and withdrawals, 
and as the number of transaction can be large, we keep these in a local database.

We create an intermediary table of transactions with their state after applying the disputes, resolves, and chargebacks.

We have to keep track of the order of the transactions so that freezing only rejects
the transactions after the freeze event, and that checks for overdraws are valid for the state of the account
at the point when the transaction is made. 
We can do that by storing an integer that increments with every incoming transaction.
We call this `ordinal`, though we can implement it as an auto-incrementing integer primary key in the database,
(assuming that items are stored in the database in the same order as they are received).

The columns we need are:
- ordinal (i64 - to maintain the order of transactions)
- client ID (u16)
- tx ID (u32)
- amount (text)
- disputed (bool)
- charged back (bool)

For deposits `amount` is positive, and for withdrawals `amount` is negative.
(We could potentially have an index on (client ID, tx ID) for quick lookup when updating a transaction).
When we come across a dispute, we change `disputed` to `true`. 
When we come across a resolve, we change `disputed` to `false`.
When we come across a chargeback, if `disputed` is `true`, we change `disputed` to `false`,
and change `charged back` to `true`. 


To get the sums for an account, we loop over the transactions for each client, in the order that they came in.
If the transaction is not disputed, we add the amount to `available` and to `total` - provided that
this does not bring the available amount to below zero.
If the transaction is disputed, we add the amount to `held` and to `total` - provided that
this does not bring the available amount to below zero, and if it's a withdrawal we subtract the amount 
from the available balance.
If the transaction has been charged back, nothing further is added to the sums, the account is marked as frozen,
and all subsequent transactions are ignored.

Unsafety
--------
The data store interacts with SQLite, which is written in C, and is therefore not subject to Rust's safety rules. 
Other dependencies have also not been checked for unsafe usage.

