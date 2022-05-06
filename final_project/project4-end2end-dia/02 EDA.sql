-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
-- MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
-- MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
-- MAGIC - **Receipts** - the cost of gas for specific transactions.
-- MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
-- MAGIC - **Tokens** - Token data including contract address and symbol information.
-- MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
-- MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
-- MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.

-- COMMAND ----------

-- MAGIC %run ./includes/utilities

-- COMMAND ----------

-- MAGIC %run ./includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Grab the global variables
-- MAGIC wallet_address,start_date = Utils.create_widgets()
-- MAGIC print(wallet_address,start_date)
-- MAGIC spark.conf.set('wallet.address',wallet_address)
-- MAGIC spark.conf.set('start.date',start_date)

-- COMMAND ----------

use ethereumetl

-- COMMAND ----------

-- MAGIC %python
-- MAGIC blocks = spark.sql('select * from blocks;')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC contracts = spark.sql("select * from contracts;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC logs = spark.sql("select * from logs;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC receipts = spark.sql("select * from receipts;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC token_prices_usd = spark.sql("select * from token_prices_usd;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC token_transfers = spark.sql("select * from token_transfers;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tokens = spark.sql("select * from tokens;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC transactions_df = spark.sql("select * from transactions;")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the maximum block number and date of block in the database

-- COMMAND ----------

select number,to_date(CAST(timestamp as TIMESTAMP)) as max_block_date from blocks where number= (select max(number) from blocks)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: At what block did the first ERC20 transfer happen?

-- COMMAND ----------

select blocks.number,from_unixtime(blocks.timestamp) from blocks where blocks.number in (select token_transfers.block_number from token_transfers,contracts where token_transfers.token_address = contracts.address and contracts.is_erc20 IS null) order by from_unixtime(blocks.timestamp) Limit 1

-- COMMAND ----------

USE ethereumetl;
SELECT MIN(block_number)
FROM token_transfers
WHERE token_address IN (SELECT address FROM silver_contracts WHERE is_erc20 = True);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: How many ERC20 compatible contracts are there on the blockchain?

-- COMMAND ----------

select count(*) from silver_contracts where is_erc20=True

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Q: What percentage of transactions are calls to contracts

-- COMMAND ----------

select sum(cast((c.address IS NOT NULL) AS INTEGER)) as to_contract,count(1) as total_transactions,sum(cast((c.address IS NOT NULL) AS INTEGER))/COUNT(1)*100 as percentage
from transactions as t left join contracts c ON c.address = t.to_address

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What are the top 100 tokens based on transfer count?

-- COMMAND ----------

select
token_address, count(distinct transaction_hash) as transfer_count
from token_transfers
group by token_address
order by transfer_count desc
limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What fraction of ERC-20 transfers are sent to new addresses
-- MAGIC (i.e. addresses that have a transfer count of 1 meaning there are no other transfers to this address for this token this is the first)

-- COMMAND ----------

-- TBD
SELECT
  SUM(CAST((transaction_count = 1) AS INTEGER)) as single_transfers,
  COUNT(1) as total_transfers,
  SUM(CAST((transaction_count = 1) AS INTEGER))/COUNT(1) as percentage
FROM (
  SELECT 
    token_address, to_address, COUNT(transaction_hash) as transaction_count
  FROM token_transfers
  GROUP BY token_address, to_address
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: In what order are transactions included in a block in relation to their gas price?
-- MAGIC - hint: find a block with multiple transactions 

-- COMMAND ----------

SELECT 
  hash, block_number, transaction_index, gas_price 
FROM transactions 
WHERE start_block>=14030000 and block_number = 14030401

-- COMMAND ----------


select "The transactions are included in a block in the order of increasing gas price - higher gas price first"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What was the highest transaction throughput in transactions per second?
-- MAGIC hint: assume 15 second block time

-- COMMAND ----------

-- TBD
select max(transaction_count)/15 AS Highest_Throughput FROM blocks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total Ether volume?
-- MAGIC Note: 1x10^18 wei to 1 eth and value in the transaction table is in wei

-- COMMAND ----------

select sum(value)/power(10,18) as total_ether_volume from ethereumetl.transactions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total gas used in all transactions?

-- COMMAND ----------

select sum(gas_used) from receipts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Maximum ERC-20 transfers in a single transaction

-- COMMAND ----------

-- TBD
SELECT 
  block_number, COUNT(transaction_hash) transfer_count
FROM token_transfers
GROUP BY block_number
ORDER BY COUNT(transaction_hash) DESC
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Token balance for any address on any date?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqlContext.setConf('spark.sql.shuffle.partitions', 'auto')
-- MAGIC from pyspark.sql.functions import col
-- MAGIC  
-- MAGIC sql_statement = "SELECT from_address, token_address, -1*SUM(value) AS Total_From_Value FROM token_transfers T \
-- MAGIC                         INNER JOIN (SELECT number FROM blocks WHERE CAST((timestamp/1e6) AS TIMESTAMP) <= '" + start_date + "') B ON B.number=T.block_number \
-- MAGIC                             GROUP BY from_address, token_address;"
-- MAGIC from_df = spark.sql(sql_statement)
-- MAGIC  
-- MAGIC sql_statement = "SELECT to_address, token_address, SUM(value) AS Total_To_Value FROM token_transfers T \
-- MAGIC                         INNER JOIN (SELECT number FROM blocks WHERE CAST((timestamp/1e6) AS TIMESTAMP) <= '" + start_date + "') B ON B.number=T.block_number \
-- MAGIC                             GROUP BY to_address, token_address;"
-- MAGIC to_df = spark.sql(sql_statement)
-- MAGIC  
-- MAGIC df = from_df.join(to_df, ((from_df.from_address == to_df.to_address) & (from_df.token_address == to_df.token_address)), 'full')
-- MAGIC df = df.na.fill(0, ['Total_To_Value']).na.fill(0, ['Total_From_Value'])
-- MAGIC df = df.withColumn('Balance', col('Total_From_Value')+col('Total_To_Value'))
-- MAGIC  
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz the transaction count over time (network use)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df_tran = spark.sql("""select to_date(CAST(`timestamp` as timestamp)) as `date`, sum(transaction_count) as transaction_count_in_a_day from blocks where year(to_date(CAST(`timestamp` as timestamp))) >= 2015 group by `date` order by `date`
-- MAGIC """)
-- MAGIC 
-- MAGIC display(df_tran)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz ERC-20 transfer count over time
-- MAGIC interesting note: https://blog.ins.world/insp-ins-promo-token-mixup-clarified-d67ef20876a3

-- COMMAND ----------

-- MAGIC %python
-- MAGIC erc_df = spark.sql("""
-- MAGIC SELECT
-- MAGIC   block_date as transfer_date,
-- MAGIC   SUM(num_transactions) total_transfers
-- MAGIC FROM (
-- MAGIC   -- This sub-query gets the date associated with the token transfers' block
-- MAGIC   SELECT
-- MAGIC     block_number, 
-- MAGIC     num_transactions,
-- MAGIC     to_date(CAST(timestamp AS TIMESTAMP)) as block_date
-- MAGIC   FROM (
-- MAGIC     -- This sub-query counts token transfers by block
-- MAGIC     SELECT
-- MAGIC       block_number,
-- MAGIC       COUNT(transaction_hash) num_transactions,
-- MAGIC       start_block, 
-- MAGIC       end_block
-- MAGIC     FROM token_transfers TT
-- MAGIC     GROUP BY block_number, start_block, end_block
-- MAGIC   ) TT
-- MAGIC   LEFT JOIN blocks B ON B.number = TT.block_number AND B.start_block >= TT.start_block AND B.end_block <= TT.end_block
-- MAGIC )
-- MAGIC GROUP BY block_date
-- MAGIC ORDER BY block_date ASC 
-- MAGIC """)
-- MAGIC display(erc_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Return Success
-- MAGIC dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
