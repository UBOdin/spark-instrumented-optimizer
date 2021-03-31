# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](https://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## How to use this Instrumented Spark

Compile with SBT the instrumented spark using command ./build/sbt package.

All graphing and testing scripts live in spark_scripts/

To generate the spark sql files from get_sql_script.py:
1. Get the TPC-H Queries along with the schema from the directory /tpch_queries.
2. Get the TPC-H benchmark data from /tpch_table
3. Change the variable createtablefile to point to the folder that has the TPC-H schemas.sql.
4. Change the csv_file variable to point to the directory that contains all the tables.
5. Create a directory to populate the spark sql commands and change the string that path lib.Path takes in to point to this directory.
6. Replace the file_name variable accordingly.
7. Change scala_write_file variable to the directory where spark sql commands should be put.
8. Run using python interpreter

To generate runs and corresponding .sh from get_sql_script.py
1. Change the variables output_command, scala_file_path and shell_file to take in appropriate directories.
2. Run using python interpreter.

To plot graphs using tpch_plotting.py
1. Change the path string in pathlib.Path(…) to point to the directory that contains the results.
2. Run using python interpreter
