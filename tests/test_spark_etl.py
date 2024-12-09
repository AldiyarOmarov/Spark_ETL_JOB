import sys
import os
import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from SPARK_ETL import get_lat_lng_from_api, update_missing_coordinates


@patch('SPARK_ETL.requests.get')
def test_get_lat_lng_from_api(mock_get):
    mock_response = {
        "results": [{
            "geometry": {
                "lat": 51.5074,
                "lng": -0.1278
            }
        }]
    }
    mock_get.return_value.json.return_value = mock_response

    lat, lng = get_lat_lng_from_api('London', 'UK')

    print(f"Latitude: {lat}, Longitude: {lng}")

    assert round(lat, 3) == 51.507
    assert round(lng, 3) == -0.128

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("Unit Test").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark

def test_update_missing_coordinates(spark):
    data = [
        (1, 101, 'Franchise 1', 1001, 'USA', 'New York', None, None),
        (2, 102, 'Franchise 2', 1002, 'Canada', 'Toronto', 43.653, -79.383),
    ]
    columns = ["id", "franchise_id", "franchise_name", "restaurant_franchise_id", "country", "city", "lat", "lng"]
    
    df = spark.createDataFrame(data, columns)

    updated_df = update_missing_coordinates(df, spark)

    updated_data = updated_df.collect()

    print(f"Updated data: {updated_data}")

    assert updated_data[0]["lat"] is not None
    assert updated_data[0]["lng"] is not None
    assert round(updated_data[1]["lat"], 3) == 43.653
    assert updated_data[1]["lng"] == pytest.approx(-79.383, rel=1e-3)
