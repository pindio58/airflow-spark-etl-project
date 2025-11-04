import sys
from pathlib import Path

# append to path to resolve import issues
BASE_DIR = Path(__file__).resolve().parent.parent
# print(BASE_DIR)

if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))


from shared.utils.commonUtils import get_logger
from shared.utils.sparkUtils import get_spark_session, read_raw_data, write_data
from shared.settings import appname, BUCKET_NAME

from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum
from pyspark.sql.window import Window

# confs spicifc to this file/dataframe
primary_columns = ['Item_Identifier', 'Outlet_Identifier']
categorical_columns = ['Item_Fat_Content', 'Outlet_Size', 'Outlet_Location_Type', 'Outlet_Type']
numerical_columns = ['Item_Weight', 'Item_Visibility', 'Item_MRP', 'Item_Outlet_Sales']

clean_folder='cleaned-version'
features_folder='features-version'
vaidated_folder='validated-version'
aggregated_folder='aggregated-version'

# data files
file_name = "BigMartSales.csv"
source_file = BASE_DIR / "shared" / "data" / file_name
# source_file = Path("/app" )/ "shared" / "data" / file_name


# get logger


# get spark session
spark = get_spark_session(appname=appname,
                          use_minio=True)

# reading data
df = read_raw_data(spark=spark, source_file=source_file)


# clean data
def clean_data(df):
    """Perform data cleaning and normalization."""
    logger = get_logger(__name__)
    logger.info("Cleaning data...")

    # Fill missing Item_Weight with average
    logger.info("Filling missing Item_Weiht with average")
    avg_weight = df.select(avg(col("Item_Weight"))).first()[0]
    logger.info(f"Item_Weight average: {avg_weight}")
    df = df.fillna({"Item_Weight": avg_weight})

    logger.info("Filling missing Outlet_Size with its mode")
    mode_outlet_size = df.select(mode(col("Outlet_Size"))).first()[0]
    logger.info(f"Outlet_Size mode: {mode_outlet_size}")
    df = df.fillna({"Outlet_Size": mode_outlet_size})

    # Standardize categorical text
    logger.info(f"Going to standardize categorical column: Item_Fat_Content")
    df = (
        df.withColumn("Item_Fat_Content", regexp_replace("Item_Fat_Content", "LF", "Low Fat"))
        .withColumn("Item_Fat_Content", regexp_replace("Item_Fat_Content", "low fat", "Low Fat"))
        .withColumn("Item_Fat_Content", regexp_replace("Item_Fat_Content", "reg", "Regular"))
    )
    
    # Drop duplicates
    logger.info("Droping duplicates based on Item_Identifier, Outlet_Identifier")
    df = df.dropDuplicates(["Item_Identifier", "Outlet_Identifier"])

    logger.info(f"Cleaned data: {df.count()} rows after cleaning")
    return df

def add_features(df):
    """Feature engineering step."""

    logger = get_logger(__name__)
    logger.info("Adding new features...")

    df = df.withColumn(
        "Item_Age",
        year(current_date()) - col("Outlet_Establishment_Year").cast("int")
    )

    df = df.withColumn(
        "Sales_Category",
        when(col("Item_Outlet_Sales") < 1000, "Low")
        .when(col("Item_Outlet_Sales") < 3000, "Medium")
        .otherwise("High")
    )

    logger.info("Features added successfully")
    return df

def validate_data(df):
    """Check and fix data quality issues."""

    logger = get_logger(__name__)
    logger.info("Validating data...")

    # Check for null primary keys
    for column in primary_columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Null values found in primary key: {column}")

    # Fill missing categorical values
    for column in categorical_columns:
        mode_value = df.groupBy(column).count().orderBy(desc("count")).first()
        # mode_value = df.select(mode(col(column))).first()[0]
        if mode_value:
            df = df.fillna({column: mode_value[0]})

    # Fill missing numerical values
    for column in numerical_columns:
        mean_value = df.select(avg(col(column))).first()[0]
        if mean_value is not None:
            df = df.fillna({column: mean_value})

    logger.info("Data validation complete")
    return df


def compute_aggregates(df):
    """Compute outlet and item-level aggregates and cumulative sales."""
    logger = get_logger(__name__)
    logger.info("Computing aggregates...")

    df = df.withColumn("Item_Outlet_Sales", col("Item_Outlet_Sales").cast("double"))

    # Total sales by outlet
    outlet_sales = (
        df.groupBy("Outlet_Identifier")
        .agg(sum("Item_Outlet_Sales").alias("total_sales"))
    )

    # Average sales by item type
    item_avg_sales = (
        df.groupBy("Item_Type")
        .agg(avg("Item_Outlet_Sales").alias("average_sale"))
    )

    # Cumulative sales per outlet
    window_spec = (
        Window.partitionBy("Outlet_Identifier")
        .orderBy(col("Item_Outlet_Sales").desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df_cumsum = df.withColumn("cumsum", _sum("Item_Outlet_Sales").over(window_spec))

    df_final = (
        df_cumsum.join(outlet_sales, "Outlet_Identifier", "left")
        .join(item_avg_sales, "Item_Type", "left")
        .select(
            "Outlet_Identifier",
            "Item_Type",
            "Item_Outlet_Sales",
            "cumsum",
            "total_sales",
            "average_sale",
        )
    )

    logger.info("Aggregations computed successfully")
    return df_final


def run_etl():

    logger = get_logger(__name__)
    df_cleaned = clean_data(df)
    logger.info(f"Saving cleaned dataframe to {BUCKET_NAME}/{clean_folder}")
    write_data(df = df_cleaned, bucket_name=BUCKET_NAME, folder=clean_folder)

    
    df_features = add_features(df_cleaned)
    logger.info(f"Saving features dataframe to {BUCKET_NAME}/{features_folder}")
    write_data(df = df_features, bucket_name=BUCKET_NAME, folder=clean_folder)


    
    df_validated = validate_data(df_features)
    logger.info(f"Saving validated dataframe to {BUCKET_NAME}/{vaidated_folder}")
    write_data(df = df_validated, bucket_name=BUCKET_NAME, folder=vaidated_folder)

  
    df_aggregated = compute_aggregates(df_validated)
    logger.info(f"Saving aggregated dataframe to {BUCKET_NAME}/{aggregated_folder}")
    write_data(df = df_aggregated, bucket_name=BUCKET_NAME, folder=aggregated_folder)

if __name__ == "__main__":
    try:
        run_etl()
    finally:
        spark.stop()  # Ensure spark session is closed even if an error occurs
