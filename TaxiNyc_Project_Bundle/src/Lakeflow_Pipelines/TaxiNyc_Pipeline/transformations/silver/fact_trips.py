from pyspark import pipelines as dp
import pyspark.sql.functions as f

catalog = spark.conf.get("catalog","taxis_project_dev")

def generate_sk(df):
    key_cols = ["vendor_id","pickup_date_id","pickup_time_id","dropoff_date_id","dropoff_time_id","pickup_longitude","pickup_latitude","dropoff_latitude","dropoff_longitude","trip_distance_km"]

    return df.withColumn("trip_sk",f.sha2(f.concat_ws("||", *key_cols),256))

def dedup(df):
    return df.drop_duplicates()

def trim_strings_cols(df):
    string_cols = [field.name for field in df.schema.fields if field.dataType.typeName() == "string"]

    for str_col in string_cols:
        #sobreescribimos la col original, no es appendear una nueva
        df = df.withColumn(str_col, f.trim(f.col(str_col)))
    
    return df

def handle_missing_values(df):
    #no podemos tener FKs nulls, las vamos a manejar dando valores especiales segun cada dim:
    df = df.withColumn("vendor_id", f.when(f.col("VendorID").isNull(),-1).otherwise(f.col("VendorID")))\
        .withColumn("ratecode_id", f.when(f.col("RatecodeID").isNull(),99).otherwise(f.col("RatecodeID")))\
        .withColumn("paymenttype_id", f.when(f.col("payment_type").isNull(),5).otherwise(f.col("payment_type")))\

    #vamos a manejar nulls de attr facts (medidas) por eso no los toco
    
    df = df.withColumn("store_and_fwd_flag",f.when(f.col("store_and_fwd_flag").isNull(),"Unknown").otherwise(f.col("store_and_fwd_flag")))

    return df

def standarize_data(df):
    #vamos a separar en date y time los timestamps
    for i in ["pickup","dropoff"]:
        df = df.withColumn(f"{i}_date_id", f.date_format(f"tpep_{i}_datetime", "yyyyMMdd"))\
        .withColumn(f"{i}_time_id", f.date_format(f"tpep_{i}_datetime", "HHmm00"))

    df = df.withColumn("loaded_at",f.current_timestamp())

    df = df.withColumn("trip_distance", f.round(f.col("trip_distance") * 1.609344, 2)) \
       .withColumnRenamed("trip_distance", "trip_distance_km")

    return df

@dp.table(
    name=f"{catalog}.silver.fact_trips",
    comment="Fact table de trips",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)
def trips_fact():
    bronze_df = spark.readStream.table(f"{catalog}.bronze.yellow_taxi")

    #primero dedup
    df = dedup(bronze_df)
    #trimm a string cols
    df = trim_strings_cols(df)
    #manejamos nulls
    df = handle_missing_values(df)
    df = standarize_data(df)
    #ultimo SK
    df = generate_sk(df)
    df = df.select(
        "trip_sk",
        "vendor_id",
        "ratecode_id",
        "paymenttype_id",
        "pickup_date_id",
        "pickup_time_id",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_date_id",
        "dropoff_time_id",
        "dropoff_longitude",
        "dropoff_latitude",
        "passenger_count",
        "trip_distance_km",
        "store_and_fwd_flag",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "loaded_at"
    )
    return df
