# Import required modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("DatabricksSyncDemo").getOrCreate()

# Sample data
data = [("James", "Smith", 30), ("Anna", "Rose", 41), ("Robert", "Williams", 62)]

# Create a DataFrame
columns = ["FirstName", "LastName", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a simple transformation
df_filtered = df.filter(col("Age") > 40)

# Show the result
df_filtered.show()

# Stop the Spark session
spark.stop()
