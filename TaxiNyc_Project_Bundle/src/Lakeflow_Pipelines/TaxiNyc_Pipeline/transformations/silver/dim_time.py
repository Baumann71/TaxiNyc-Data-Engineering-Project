from pyspark import pipelines as dp
from pyspark.sql import functions as f

#
catalog = spark.conf.get("catalog", "taxis_project_dev")  # Default manual


@dp.materialized_view(
    name= f"{catalog}.silver.dim_time",
    comment="dim time table para el pickup, dropoff",
    table_properties={
        "layer":"silver",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)
def min_gen():
    df = spark.sql("""
        select explode(sequence(
            timestamp '1970-01-01 00:00:00',
            timestamp '1970-01-01 23:59:00',
            interval 1 minute
        )) as time_stamp  
    """)

    df = df.withColumn("time_id", f.date_format("time_stamp","HHmmss"))\
        .withColumn("hour", f.hour("time_stamp"))\
        .withColumn("minute", f.minute("time_stamp"))\
        .withColumn("second", f.second("time_stamp"))\
        .withColumn("time_hhmmss", f.date_format("time_stamp","HH:mm:ss"))
    
    return df.select(
        "time_id",
        "time_hhmmss",
        "hour",
        "minute",
        "second"
    )