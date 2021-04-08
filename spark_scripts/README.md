1. Run tpch_plotting.py to generate graphs. Pre-generated results are present in "/tpch/tpch_output/" 
	1. Change lines 8, 9 and 10 to reflect appropriate names and folder locations. The default has already been set up.
	2. To re-generate results from scratch follow steps 2 and 3 and re-run step 1.
2. Generate the spark SQL and shell scripts from get_sql_script.py:
	1. Change lines 13, 14, 15, 16, 17, 18 and 69 to point to the appropriate directories. Numbers are average over 5 runs. 
	2. The default directories and scripts are already setup. 
3. Run shell scripts present in "/tpch/tpch_spark_shell_script_i.sh" from spark/ to average over 5 runs. If not specified in python file default directories and scripts will be used.
