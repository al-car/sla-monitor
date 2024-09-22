
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_sub, dayofweek, when
from pyspark.sql.types import DateType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Date Adjustment") \
    .getOrCreate()

# Define HDFS paths
input_path = "hdfs://path/to/your/input/data"
output_path = "hdfs://path/to/your/output/data"



# Define the number of iterations (5 * 52 * 3 = 780)
num_iterations = 780

# Define the initial date
current_date = "2024-09-09"

for _ in range(num_iterations):
    # Load the DataFrame from HDFS (in Parquet format) for the current date
    df = spark.read.parquet(input_path).filter(col("date") == current_date)

    # Iterate through the DataFrame columns, identify date columns, and subtract one day
    for column, dtype in df.dtypes:
        if dtype == 'date':
            df = df.withColumn(column, 
                when(dayofweek(col(column)).isin([2, 3, 4, 5]), date_sub(col(column), 1))
                .when(dayofweek(col(column)) == 1, date_sub(col(column), 3))
                .when(dayofweek(col(column)) == 7, date_sub(col(column), 2))
                .otherwise(col(column))
            )

    # Save the modified DataFrame back to HDFS in Parquet format
    df.write.mode('append').parquet(output_path)

    # Update the current_date for the next iteration
    current_date = df.select(col("date")).first()[0]


# Connect to Impala and refresh the table
from impala.dbapi import connect
from impala.util import as_pandas

# Impala connection details
impala_host = "your_impala_host"
impala_port = 21050  # Default Impala port
impala_user = "your_username"
impala_password = "your_password"

# Establish connection to Impala
conn = connect(host=impala_host, port=impala_port, user=impala_user, password=impala_password)
cursor = conn.cursor()

try:
    # Refresh the "data" table
    cursor.execute("REFRESH data")
    
    # Compute statistics for the "data" table
    cursor.execute("COMPUTE STATS data")
    
    print("Table 'data' refreshed and statistics computed successfully.")
except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()


# Stop the Spark session
spark.stop()
