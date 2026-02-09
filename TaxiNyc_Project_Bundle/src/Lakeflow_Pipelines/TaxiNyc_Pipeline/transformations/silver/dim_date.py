from pyspark import pipelines as dp
from pyspark.sql import functions as f

#
catalog = spark.conf.get("catalog", "taxis_project_dev")  # Default manual


#usamos estos parametros para desarrollar la tabla
start_date = spark.conf.get("start_date","2016-03-01")
end_date = spark.conf.get("end_date","2016-03-31")

#es una dim estatica, se va a cargar datos rara vez para extender fecha
#por ende no requiere un incremental estricto como una streaming
@dp.materialized_view(
    name=f"{catalog}.silver.dim_date",
    comment="date dimension love u kimball",
    table_properties={
        "layer":"silver",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)
def calendar():
    df = spark.sql(
        f"""
            select explode(sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )) as date
        """
    )

    df = df.withColumn(
        "date_id", f.date_format(f.col("date"), "yyyyMMdd").cast("int")
    )

    df = df.withColumn("year",f.year(f.col("date")))\
        .withColumn("month",f.month(f.col("date")))\
        .withColumn("quarter", f.quarter(f.col("date")))
    
    df = df.withColumn("day_of_month", f.dayofmonth(f.col("date")))\
        .withColumn("day_of_week",f.date_format(f.col("date"),"EEEE"))\
        .withColumn("day_of_week_abbr",f.date_format(f.col("date"),"EEE"))\
        .withColumn("day_of_week_num",f.dayofweek(f.col("date")))
    
    df = df.withColumn("month_name",f.date_format(f.col("date"),"MMMM"))\
        .withColumn("month_year", 
            f.concat(f.date_format(f.col("date"),"MMMM"), f.lit(" "), f.col("year"))
        )\
        .withColumn("quarter_year", 
            f.concat(f.lit("Q"),f.col("quarter"), f.lit(" "), f.col("year"))
        )

    df = df.withColumn("week_of_year", f.weekofyear(f.col("date")))\
        .withColumn("day_of_year",f.dayofyear(f.col("date")))
    
    df = df.withColumn("is_weekend", f.when(f.col("day_of_week_num").isin([1,7]),True).otherwise(False))

    return df.select(
        "date_id",    
        "date",        
        "year",
        "quarter",
        "quarter_year",
        "month",
        "month_name",
        "month_year",
        "day_of_month",
        "day_of_year",
        "day_of_week_num",
        "day_of_week",
        "day_of_week_abbr",
        "week_of_year",
        "is_weekend"
    )