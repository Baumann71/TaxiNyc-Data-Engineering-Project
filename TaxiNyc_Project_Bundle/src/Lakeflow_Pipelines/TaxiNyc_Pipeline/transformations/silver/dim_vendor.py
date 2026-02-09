from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

catalog = spark.conf.get("catalog","taxis_project_dev")

@dp.materialized_view(
    name=f"{catalog}.silver.dim_vendor",
    comment="Vendor - hardcode del data dictionary TLC"
)
def vendor_dim():
    bronze_df =  spark.read.table(f"{catalog}.bronze.yellow_taxi")
    bronze_df.createOrReplaceTempView("bronze_df")
    df = spark.sql("""
        with vendors as (
            select distinct VendorId as vendor_id
            from bronze_df
            where vendorID is not null
        )       

        select vendor_id,
            case 
                when vendor_id = 1 then "Creative Mobile Technologies, LLC"
                when vendor_id = 2 then "Curb Mobility, LLC"
                when vendor_id = 6 then "Myle Technologies Inc"
                when vendor_id = 7 then "Helix"
                when vendor_id = -1 then "Unknown/Null" -- para controlar nulls en fks de la fact, nunca hay q tneer nulls en la fk de la fact
            end as vendor_description
        from vendors            
    """)

    df = df.withColumn("loaded_at", current_timestamp())

    return df