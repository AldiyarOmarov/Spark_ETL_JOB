# Spark_ETL_JOB
Spark ETL job to read data from a local storage, fill missing lng and lat using OpenCage. Join two dataframes on geohash.
# Repo link
https://github.com/AldiyarOmarov/Spark_ETL_Job

## Overview
This project demonstrates an ETL (Extract, Transform, Load) pipeline built using PySpark. The pipeline processes restaurant data, retrieves missing geographical coordinates using the OpenCage Geocoding API, and merges weather data based on geohash. Finally, it saves the cleaned and processed data in Parquet format.

## Features:
- **Geographical Data**: Missing latitude and longitude values for restaurants are fetched from the OpenCage Geocoding API based on the city and country.
- **Weather Data Integration**: Weather data is joined with restaurant data using geohash.
- **Output**: Processed data is saved in Parquet format, partitioned by the city.

## .env:
Create a .env file in the root directory and add your OpenCage API key:\
OPENCAGE_API_KEY="your_api_key" \
RESTAURANT_PATH= path to your restaurant csv \
WEATHER_PATH= path to your weather parquet \
OUTPUT_PATH= path to your output folder 


## Screenshots
Visual representation of executing code lines like:\
joined_df.show()\
print(f"Number of rows in the joined DataFrame: {joined_df.count()}")

Pytests and folder "output"
## Requirements

To run this project, you need the following:

- **Python**
- **Apache Spark**
- **PySpark**
- **requests**
- **geohash2**
- **python-dotenv**
- **pytest**
- **pytest-mock**

You can install the required Python packages by running:

```bash
pip install -r requirements.txt

