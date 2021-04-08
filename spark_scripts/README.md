Compile the instrumented spark by running ./build/sbt package.

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
1. Change the input file path to the folder that has the result of running the tpch queries on spark. The default results live in "tpch_output/tpch_output_". #line 8
2. Change the name of the output files. The default name is "/output_". #line 9
3. Change the type of output file. The default type of the ouputfile is ".txt". #line 10
2. Run using python interpreter
