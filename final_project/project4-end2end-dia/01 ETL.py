# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE...

# COMMAND ----------

# MAGIC %md
# MAGIC displayHTML("<img src ='https://github.com/tapanpradyot/dscc202-402-spring2022/blob/main/data_pipeline.jpg?raw=true'>") 

# COMMAND ----------

# MAGIC %sql
# MAGIC use ethereumetl;

# COMMAND ----------

sqlContext.setConf('spark.sql.shuffle.partitions', 'auto')
from pyspark.sql.functions import col, coalesce

# COMMAND ----------

sqlCMD1 = """
SELECT tableType.*,price.*, tableType.Value*price.price_usd AS totalValue_usd
    FROM ((SELECT DISTINCT * FROM token_prices_usd) AS price)
        INNER JOIN (token_transfers AS tableType) ON tableType.token_address = price.contract_address
            WHERE price.asset_platform_id == 'ethereum'
"""

sqlCMDs = """
SELECT *
    FROM G03_db.silverTransactions
        INNER JOIN silver_contracts
            ON silver_contracts.address = silverTransactions.token_address
                WHERE silver_contracts.is_erc20 == 'True'
"""


sqlCMD2 = """
SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as w_ID, w_hash FROM
    (SELECT DISTINCT col1 AS w_hash FROM
        (SELECT from_address as col1 FROM G03_db.silverERC20) 
        UNION
        (SELECT to_address as col1 FROM G03_db.silverERC20))
"""

sqlCMD3 = """
SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS t_ID, * FROM 
    (SELECT DISTINCT symbol, name,asset_platform_id,description, links, image, price_usd, contract_address,sentiment_votes_up_percentage,sentiment_votes_down_percentage,market_cap_rank,coingecko_rank,coingecko_score,developer_score,community_score,liquidity_score,public_interest_score
        FROM token_prices_usd
            WHERE asset_platform_id == 'ethereum')
"""



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating a silver table: silverTransactions 
# MAGIC ##### which contains unique etherium only transactions and is created 
# MAGIC ##### using an inner join of the table token_transfers(token_address) and token_prices_usd(contract_address)
# MAGIC ##### 

# COMMAND ----------

df1 = spark.sql(sqlCMD1)
df1.write.mode('overwrite').option('mergeSchema', 'true').partitionBy('start_block', 'end_block').saveAsTable('G03_db.silverTransactions')

# COMMAND ----------

df4 = spark.sql(sqlCMDs)
df4.write.mode('overwrite').option('mergeSchema', 'true').partitionBy('start_block', 'end_block').saveAsTable('G03_db.silverERC20')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating a silver table: silverWalletIds 
# MAGIC ##### created from our silver table silverTransactions
# MAGIC ##### contains all the unique tokens and information involved in the etherium only transactions

# COMMAND ----------

df2 = spark.sql(sqlCMD2)
df2.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('G03_db.silverWalletIds')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Creating a silver table: silverTokensEth
# MAGIC ##### created from original table token_prices_usd
# MAGIC ##### contains all the unique wallet ids involved in the etherium only transactions (both from and to address)

# COMMAND ----------

df3 = spark.sql(sqlCMD3)
df3.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('G03_db.silverTokensEth')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating a silver table: silverBal
# MAGIC ##### 
# MAGIC ##### 

# COMMAND ----------

sqlCMD4 = """
SELECT from_address, token_address AS from_token_address, -SUM(totalValue_usd) AS from_total 
    FROM G03_db.silverERC20 
        GROUP BY from_address, token_address;
"""


sqlCMD5 = """
SELECT to_address, token_address AS to_token_address, SUM(totalValue_usd) AS to_total
    FROM G03_db.silverERC20
        GROUP BY to_address, token_address;
"""


dfFrom = spark.sql(sqlCMD4)
dfTo = spark.sql(sqlCMD5)
tokenData = spark.sql('SELECT * FROM G03_db.silverTokensEth')
walletData = spark.sql('SELECT * FROM G03_db.silverWalletIds')

# COMMAND ----------

df = dfFrom.join(dfTo, ((dfFrom.from_address == dfTo.to_address) & (dfFrom.from_token_address == dfTo.to_token_address)), 'full')
df = df.na.fill(0, ['to_total']).na.fill(0, ['from_total'])
df = df.withColumn('Balance', col('from_total')+col('to_total'))

df = df.withColumn('WalletAddress', coalesce(df['from_address'], df['to_address']))
df = df.withColumn('TokenAddress', coalesce(df['from_token_address'], df['to_token_address']))
df = df.drop(*('from_address', 'to_address', 'from_token_address', 'to_token_address'))

df = df.join(tokenData, (df.TokenAddress == tokenData.contract_address), 'inner')
df = df.join(walletData, (df.WalletAddress == walletData.w_hash), 'inner')
df.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('G03_db.silverBal')

# COMMAND ----------

# MAGIC %md
# MAGIC converting tables to delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA G03_db.silverTransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA G03_db.silverWalletIds

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA G03_db.silverTokensEth

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA G03_db.silverERC20

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA G03_db.silverBal

# COMMAND ----------

# MAGIC %python
# MAGIC # Return Success
# MAGIC dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


