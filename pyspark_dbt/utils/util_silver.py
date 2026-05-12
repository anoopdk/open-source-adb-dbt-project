from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, row_number, desc
from pyspark.sql.window import Window
from functools import reduce
from typing import List
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

class silver_transformations:
    def __init__(self, df):
        self.df = df
    
    def silver_dedup(self, df:DataFrame, dedup_cols:List,cdc_cols:str) -> DataFrame:
        df = df.withColumn("dedupKeys", concat(*dedup_cols))
        df = df.withColumn("row_filter", row_number().over(Window.partitionBy("dedupKeys").orderBy(desc(cdc_cols))))
        df = df.filter(df.row_filter == 1).drop("dedupKeys", "row_filter")
        return df
    
    def silver_not_nulls(self, df:DataFrame, not_null_cols:List) -> DataFrame:
        df = df.filter(reduce(lambda a, b: a & b, [df[c].isNotNull() for c in not_null_cols]))
        return df
    
    def silver_process_timestamp(self, df:DataFrame) -> DataFrame:        
        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    
    def silver_upsert(self, spark, df:DataFrame, key_cols:List, table_name:str, cdc_cols:List) -> int:
        dlt_obj = DeltaTable.forName(spark,f"uber_pyspark_dbt.silver.{table_name}")
        merge_condition = " AND ".join([f"s.{col} = t.{col}" for col in key_cols])
        dlt_obj.alias("t").merge(
            source=df.alias("s"),
            condition=merge_condition
        )\
        .whenMatchedUpdateAll(condition=" AND ".join([f"s.{col} >= t.{col}" for col in cdc_cols]))\
        .whenNotMatchedInsertAll()\
        .execute()
        return 1