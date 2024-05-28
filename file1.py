from pyspark.sql import *
# The line findspark.init() in your code fixes the "Python worker failed to connect back" error because it helps Spark locate your Python installation and environment. Here's a breakdown of what findspark.init() does:
#
# Searches for PySpark: It searches your system for the PySpark library and adds its path to the Python sys.path. This ensures Spark can find the necessary PySpark modules to interact with your Python environment.
#
# Environment Variable Configuration (Optional): In some cases, findspark.init() might also help set environment variables like PYSPARK_DRIVER_PYTHON or PYSPARK_PYTHON if they are not already configured. These variables specify the Python interpreter that Spark should use.
#
# Why it Fixes the Error:
#
# Before running findspark.init(), Spark might not be able to find the PySpark library or the correct Python interpreter. This can lead to communication issues between Spark and your Python code, resulting in the "Python worker failed to connect back" error.
#
# By adding PySpark to the system path and potentially setting environment variables, findspark.init() bridges the gap between Spark and your Python environment, allowing them to connect and work together successfully.
#
# Alternative Approach:
#
# While findspark.init() simplifies the process, you can also achieve the same functionality by manually setting the PYSPARK_DRIVER_PYTHON and PYSPARK_PYTHON environment variables to the path of your desired Python interpreter. This approach might be preferred if you have specific requirements for your Python environment.
import findspark
findspark.init()

if __name__ == "__main__":

    spark = SparkSession.builder \
            .appName("hello spark") \
            .master("local[2]") \
            .getOrCreate()

    data_list = [
        ("ravi", 28),
        ("davud", 35),
        ("shubh", 74)
    ]

    df = spark.createDataFrame(data_list).toDF("name","age")
    df.show()
