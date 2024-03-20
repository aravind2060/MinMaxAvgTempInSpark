import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def initialize_spark(app_name,master_node_url):
    """
    Initialize and return a Spark session
    """
    return SparkSession.builder.appName(app_name).master(master_node_url).getOrCreate()

def read_data(spark, input_file, schema):
    """
    Read and return data from the CSV file using the provided Spark session and schema
    """
    return spark.read.csv(input_file, header=False, schema=schema)

def filter_temperature_data(df, year_month):
    """
    Filter data for the given month and temperature elements (TMAX, TMIN)
    """
    return df.filter(
        (col("DATE").startswith(year_month)) &
        (col("ELEMENT").isin("TMAX", "TMIN"))
    )

def calculate_average_temperatures(filtered_df):
    """
    Calculate and return the average temperatures
    """
    return filtered_df.groupBy("ID", "ELEMENT").agg(
        avg("DATA_VALUE").alias("Avg_Temperature")
    )

def save_results(result_df, output_file):
    """
    Save the result DataFrame to the specified output file
    """
    result_df.write.csv(output_file, header=True)

def main(master_node_url,year_month, input_file, output_file):
    
    spark = initialize_spark("Temperature Analysis",master_node_url)

    schema = "ID STRING, DATE STRING, ELEMENT STRING, DATA_VALUE INT, M_FLAG STRING, Q_FLAG STRING, S_FLAG STRING, OBS_TIME STRING"
    data_df = read_data(spark, input_file, schema)

    temperature_df = filter_temperature_data(data_df, year_month)
    avg_temperature_df = calculate_average_temperatures(temperature_df)

    save_results(avg_temperature_df, output_file)

    # this infinte while loop when u want to see spark context for viewing DAG
    # while True:
    #     pass
    stopSparkSession(spark)
def stopSparkSession(spark):
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python TemperatureAnalysis.py <year_month> <input_file> <output_file>\n Ex: /spark/bin/spark-submit TemperatureAnalysis.py /data/2024.csv /data/out")
        sys.exit(-1)

    year_month, input_file, output_file = sys.argv[1:4]
    master_node_url="spark://spark-master:7077";
    main(master_node_url,year_month, input_file, output_file)
