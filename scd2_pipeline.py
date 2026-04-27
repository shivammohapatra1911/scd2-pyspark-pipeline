# =========================
# Imports
# =========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# =========================
# READ DATA
# =========================
def read_data(spark):

    customer_dim_data = [
        (1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
        (2,'vikash','patna','india','Y','2023-08-12',None),
        (3,'nikita','delhi','india','Y','2023-09-10',None),
        (4,'rakesh','jaipur','india','Y','2023-06-10',None),
        (5,'ayush','NY','USA','Y','2023-06-10',None),
        (1,'manish','gurgaon','india','Y','2022-09-25',None),
    ]

    schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("active", StringType(), True),
    StructField("effective_start_date", StringType(), True),
    StructField("effective_end_date", StringType(), True)
])


    customer_dim_df = spark.createDataFrame(customer_dim_data, schema)

    sales_data = [
        (1,1,'manish','2023-01-16','gurgaon','india',380),
        (77,1,'manish','2023-03-11','bangalore','india',300),
        (12,3,'nikita','2023-09-20','delhi','india',127),
        (54,4,'rakesh','2023-08-10','jaipur','india',321),
        (65,5,'ayush','2023-09-07','mosco','russia',765),
        (89,6,'rajat','2023-08-10','jaipur','india',321)
    ]

    sales_schema = ['sales_id','customer_id','customer_name','sales_date',
                    'food_delivery_address','food_delivery_country','food_cost']

    sales_df = spark.createDataFrame(sales_data, sales_schema)

    return sales_df, customer_dim_df


# =========================
# TRANSFORM (YOUR LOGIC)
# =========================
def process_scd2(sales_df, customer_dim_df):

    join_df = sales_df.join(
        customer_dim_df,
        sales_df["customer_id"] == customer_dim_df["id"],
        "left"
    )

    # OLD RECORDS
    old_records = join_df.filter(
        (col("food_delivery_address") != col("city")) & 
        (col("active") == "Y")
    ).withColumn("active", lit("N")) \
     .withColumn("effective_end_date", col("sales_date")) \
     .select(
         col("customer_id").alias("id"),
         col("customer_name").alias("name"),
         col("city"),
         col("country"),
         col("active"),
         col("effective_start_date"),
         col("effective_end_date")
     )

    # NEW RECORDS
    new_records = join_df.filter(
        (col("food_delivery_address") != col("city")) & 
        (col("active") == "Y")
    ).withColumn("active", lit("Y")) \
     .withColumn("effective_start_date", col("sales_date")) \
     .withColumn("effective_end_date", lit(None)) \
     .select(
         col("customer_id").alias("id"),
         col("customer_name").alias("name"),
         col("food_delivery_address").alias("city"),
         col("food_delivery_country").alias("country"),
         col("active"),
         col("effective_start_date"),
         col("effective_end_date")
     )

    # NEW CUSTOMERS
    coming_records = sales_df.join(
        customer_dim_df,
        sales_df["customer_id"] == customer_dim_df["id"],
        "leftanti"
    ).withColumn("active", lit("Y")) \
     .withColumn("effective_start_date", col("sales_date")) \
     .withColumn("effective_end_date", lit(None)) \
     .select(
         col("customer_id").alias("id"),
         col("customer_name").alias("name"),
         col("food_delivery_address").alias("city"),
         col("food_delivery_country").alias("country"),
         col("active"),
         col("effective_start_date"),
         col("effective_end_date")
     )

    # FINAL UNION
    columns = [
    "id","name","city","country",
    "active","effective_start_date","effective_end_date"
   ]
    
    old_records = old_records.select(
    col("id"),
    col("name"),
    col("city"),
    col("country"),
    col("active"),
    col("effective_start_date"),
    col("effective_end_date")
 )
    new_records = new_records.select(
    col("id"),
    col("name"),
    col("city"),
    col("country"),
    col("active"),
    col("effective_start_date"),
    col("effective_end_date")
)
    coming_records = coming_records.select(
    col("id"),
    col("name"),
    col("city"),
    col("country"),
    col("active"),
    col("effective_start_date"),
    col("effective_end_date")
)
    print("customer_dim_df:", customer_dim_df.columns, len(customer_dim_df.columns))
    print("old_records:", old_records.columns, len(old_records.columns))
    print("new_records:", new_records.columns, len(new_records.columns))
    print("coming_records:", coming_records.columns, len(coming_records.columns))
    customer_dim_df = customer_dim_df.select(columns)
    final_df = customer_dim_df.union(old_records) \
                         .union(new_records) \
                         .union(coming_records)
    

    return final_df


# =========================
# GET LATEST ACTIVE
# =========================
def get_latest_active(final_df):

    window = Window.partitionBy("id").orderBy(col("effective_start_date").desc())

    latest_active_df = final_df.filter(col("active") == "Y") \
        .withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    return latest_active_df


# =========================
# MAIN
# =========================
def main():

    spark = SparkSession.builder \
        .appName("SCD2 Pipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    sales_df, customer_dim_df = read_data(spark)

    final_df = process_scd2(sales_df, customer_dim_df)

    print("===== FINAL SCD2 TABLE =====")
    final_df.show(truncate=False)

    latest_active_df = get_latest_active(final_df)

    print("===== LATEST ACTIVE RECORDS =====")
    latest_active_df.show(truncate=False)


# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    main()