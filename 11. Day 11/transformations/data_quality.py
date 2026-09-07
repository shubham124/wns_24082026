from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(name="bronze_orders",
           comment="Raw Customer Data")
def load_orders():
    orders_df = spark.readStream.format("cloudFiles").option("cloudFiles.format","JSON").load(path="/Volumes/wns24082026/quickstart_schema/sandbox/datasets/e-commerce/staging/orders/")
    # Metadata columns
    metadata_df = orders_df.withColumn("_ingest_timestamp", expr("current_timestamp()")).withColumn("_source_file", col("_metadata.file_path"))
    # Data Quality flags
    data_quality_df = metadata_df.withColumn("dq_customer_missing", col("customer_id").isNull()) \
        .withColumn("dq_invalid_quantity", col("qty")<=0) \
        .withColumn("dq_invalid_price", col("price")<=0)
    return data_quality_df

@dp.table(name="silver_orders")
@dp.expect("valid_customers","customer_id IS NOT NULL")
@dp.expect("valid_qty","qty >0")
@dp.expect_or_drop("valid_order_ts", "order_ts <= current_date()")
def load_orders():
    df = spark.readStream.table("bronze_orders").filter(col("order_id").isNotNull()).withColumn("total_amount", col("qty")*col("price"))
    return df