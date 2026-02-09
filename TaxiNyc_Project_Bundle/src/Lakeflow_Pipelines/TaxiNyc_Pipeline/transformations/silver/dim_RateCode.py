from pyspark import pipelines as dp
from pyspark.sql import functions as f

catalog = spark.conf.get("catalog","taxis_project_dev")

#es una tabla de baja cardinalidad
@dp.materialized_view(
    name=f"{catalog}.silver.dim_ratecode",
    comment="RateCode - hardcode del data dictionary TLC"
)
def ratecode_dim():
    bronze_df =  spark.read.table(f"{catalog}.bronze.yellow_taxi")
    bronze_df.createOrReplaceTempView("bronze_df")


    df = spark.sql("""
        with rate_codes as (
            select distinct RatecodeID as rateCode_id  
            from bronze_df 
            where RatecodeID is not null    
        )

        select rateCode_id,
            case 
                when rateCode_id = 1 then 'Standard rate'
                when rateCode_id = 2 then 'JFK'
                when rateCode_id = 3 then 'Newark'
                when rateCode_id = 4 then 'Nassau or Westchester'
                when rateCode_id = 5 then 'Negotiated fare'
                when rateCode_id = 6 then 'Group ride'
                when rateCode_id = 99 then 'Null/unknown'
            end as rateCode_description
        from rate_codes
    """)

    df = df.withColumn("loaded_at", f.current_timestamp())
    
    return df

    
