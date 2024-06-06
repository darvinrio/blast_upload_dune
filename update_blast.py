import pandas as pd
from dotenv import dotenv_values
from shroomdk import ShroomDK
from dune_client.client import DuneClient


config = dotenv_values(".env")

SHROOM_KEY = config['SHROOM_KEY']
DUNE_API_KEY = config['DUNE_API_KEY']
NAMESPACE = config['NAMESPACE']

# setup Flipside
sdk = ShroomDK(SHROOM_KEY)
def get_result_from_sql(sql):
  query_result_set = sdk.query(sql)
  results = query_result_set.records
  return results

# get Blast data
blast_gas_query = """
select 
  DATE_PART(epoch_second, date(block_timestamp)) as unixtimestamp,
  date(block_timestamp) as date,
  sum(tx_fee) as fee
from blast.core.fact_transactions
group by 1,2
order by 1,2
"""

blast_gas_result = get_result_from_sql(blast_gas_query)
blast_gas_df = pd.DataFrame(blast_gas_result)

blast_gas_df = blast_gas_df.drop(['__row_index'], axis = 1)
blast_gas_df.to_csv('blast_dataset.csv', index=False)
blast_gas_csv = blast_gas_df.to_csv(index=False)

# setup Dune
dune = DuneClient(DUNE_API_KEY)

table = dune.upload_csv(
    data=str(blast_gas_csv),
    description="blast fees",
    table_name="blast_fees_daily",
    is_private=False
)
# creates table called `dataset_blast_fees_daily`