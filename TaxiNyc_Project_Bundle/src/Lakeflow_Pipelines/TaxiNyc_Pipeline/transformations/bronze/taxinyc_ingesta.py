from pyspark import pipelines as dp
import pyspark.sql.functions as f

catalog = spark.conf.get("catalog", "taxis_project_dev")  # Default manual
source_path=f"/Volumes/{catalog}/bronze/raw_data/"

#streaming table
#le damos propiedades a la delta table
@dp.table( 
    name=f"{catalog}.bronze.yellow_taxi",
    comment="tabla raw as is, sobre archivos csv manejados con auto loader",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "source_fomrat":"csv",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)
def ingest_data():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
        .option("header","true")\
        .option("cloudFiles.inferColumnTypes","true")\
        .option("cloudFiles.schemaEvolutionMode","rescue")\
        .option("cloudFiles.maxFilesPerTrigger","2")\
        .load(source_path)
    
    
    df = df.withColumn("ingested_timestamp", f.current_timestamp())\
        .withColumn("source_path",f.col("_metadata.file_path"))

    return df