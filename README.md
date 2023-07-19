# Data Project Task

## Project Dependencies
- pandas
- sqlite
- protobuf

## Instructions
- `transactions_parsing.py` - deserializes transaction_pb2.py file created from transaction.proto file and populates SQLite transactions table.
- `settlements_withdrawals_parsing.py` - parses the settlementswithdrawals.txt file and populates settlements and withdrawals tables.
- `csv_creator.py` - executes select queries and creates 3 .csv files from those results.
- `create_database.py` - contains DDL for 3 SQLite DB tables
- `sql_ddl_select_statements.sql` - contains DDL and queries exported from DBeaver
- `test.db` - SQLite DB