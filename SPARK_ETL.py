import os
import glob
import requests

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, coalesce, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType
import geohash2


# Load the API key and paths from .env
load_dotenv()

restaurant_path = os.getenv("RESTAURANT_PATH")
weather_path = os.getenv("WEATHER_PATH")
output_path = os.getenv("OUTPUT_PATH")

def get_lat_lng_from_api(city, country):
    api_key = os.getenv("OPENCAGE_API_KEY")
    api_url = "https://api.opencagedata.com/geocode/v1/json"
    params = {
        "q": f"{city}, {country}",
        "key": api_key,
        "limit": 1,
    }
    try:
        response = requests.get(api_url, params=params)
        data = response.json()
        if data["results"]:
            lat = data["results"][0]["geometry"]["lat"]
            lng = data["results"][0]["geometry"]["lng"]
            return lat, lng
    except Exception as e:
        print(f"Error while fetching data from API: {e}")
    return None, None


def update_missing_coordinates(missing_df, spark):
    updated_data = []
    for row in missing_df.collect():
        city = row["city"]
        country = row["country"]
        lat, lng = get_lat_lng_from_api(city, country)
        lat = round(lat, 3) if lat else None
        lng = round(lng, 3) if lng else None
        updated_data.append(
            (row["id"], row["franchise_id"], row["franchise_name"],
             row["restaurant_franchise_id"], row["country"], row["city"],
             lat, lng)
        )
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("franchise_id", IntegerType(), True),
        StructField("franchise_name", StringType(), True),
        StructField("restaurant_franchise_id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
    ])
    return spark.createDataFrame(updated_data, schema)

@udf(StringType())
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        return geohash2.encode(lat, lng, precision=4)
    return None


def main():
    #Increased memory to 10gb
    spark = SparkSession.builder \
        .appName("Spark ETL Job") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.memory", "10g") \
        .getOrCreate()

    global weather_path  
    weather_path = glob.glob(weather_path + "/**/*/*.parquet", recursive=True)

    restaurant_df = spark.read.csv(restaurant_path, header=True, inferSchema=True)
    restaurant_df.show()
    print(f"Number of rows in the restaurant DataFrame: {restaurant_df.count()}")

    weather_df = spark.read.parquet(*weather_path)
    weather_df.show()
    print(f"Number of rows in the weather DataFrame: {weather_df.count()}")

    missing_df = restaurant_df.filter((restaurant_df["lat"].isNull()) | (restaurant_df["lng"].isNull()))
    missing_df.show()
    print(f"Number of rows in the missing DataFrame: {missing_df.count()}")

    updated_df = update_missing_coordinates(missing_df, spark)

    updated_df_aliased = updated_df.select(
        col("id").alias("updated_id"), col("lat").alias("updated_lat"), col("lng").alias("updated_lng")
    )
    final_df = restaurant_df.join(updated_df_aliased, restaurant_df["id"] == updated_df_aliased["updated_id"], "left") \
                            .withColumn("lat", coalesce(col("updated_lat"), col("lat"))) \
                            .withColumn("lng", coalesce(col("updated_lng"), col("lng"))) \
                            .drop("updated_lat", "updated_lng", "updated_id")

    final_df = final_df.dropDuplicates()
    final_df = final_df.withColumn("geohash", generate_geohash(col("lat"), col("lng")))

    weather_df = weather_df.withColumn("geohash", generate_geohash(col("lat"), col("lng")))
    weather_df = weather_df.withColumnRenamed("lat", "weather_lat") \
                           .withColumnRenamed("lng", "weather_lng")

    joined_df = final_df.join(weather_df, "geohash", "left")
    joined_df = joined_df.dropDuplicates(["id", "geohash"])

    joined_df.show()
    print(f"Number of rows in the joined DataFrame: {joined_df.count()}")

    joined_df.write \
        .mode("overwrite") \
        .partitionBy("City") \
        .parquet(output_path)

    print(f"Data saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
