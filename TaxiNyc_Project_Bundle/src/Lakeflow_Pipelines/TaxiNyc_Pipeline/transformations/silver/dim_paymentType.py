from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

catalog = spark.conf.get("catalog","taxis_project_dev")

@dp.materialized_view(
    name=f"{catalog}.silver.dim_paymentType",
    comment="Payment - hardcode del data dictionary TLC"
)
def payment_dim():
    bronze_df = spark.read.table(f"{catalog}.bronze.yellow_taxi")
    bronze_df.createOrReplaceTempView("bronze_df")

    df = spark.sql("""
        with payments as(
            select distinct payment_type
            from bronze_df
            where payment_type is not null
        )

        select payment_type as paymenttype_id,
            case 
                when payment_type = 0 then 'Flex Fare trip'
                when payment_type = 1 then 'Credit card'
                when payment_type = 2 then 'Cash'
                when payment_type = 3 then 'No charge'
                when payment_type = 4 then 'Dispute'
                when payment_type = 5 then 'Unknown'
                when payment_type = 6 then 'Voided trip'
            end as payment_description
        from payments
    """)

    df = df.withColumn("loaded_at",current_timestamp())

    return df