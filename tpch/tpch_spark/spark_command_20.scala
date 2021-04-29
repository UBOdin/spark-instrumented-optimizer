println("Run SQL Query 20 ")
spark.sql("CREATE TABLE LINEITEM (l_orderkey INT,l_partkey INT,l_suppkey INT,l_linenumber INT,l_quantity DECIMAL,l_extendedprice DECIMAL,l_discount DECIMAL,l_tax DECIMAL,l_returnflag CHAR(1),l_linestatus CHAR(1),l_shipdate DATE,l_commitdate DATE,l_receiptdate DATE,l_shipinstruct CHAR(25),l_shipmode CHAR(10),l_comment VARCHAR(44)) USING csv OPTIONS(path './tpch/tpch_table/lineitem.tbl', delimiter '|')")
spark.sql("CREATE TABLE ORDERS (o_orderkey INT,o_custkey INT,o_orderstatus CHAR(1),o_totalprice DECIMAL,o_orderdate DATE,o_orderpriority CHAR(15),o_clerk CHAR(15),o_shippriority INT,o_comment VARCHAR(79)) USING csv OPTIONS(path './tpch/tpch_table/orders.tbl', delimiter '|')")
spark.sql("CREATE TABLE PART (p_partkey INT,p_name VARCHAR(55),p_mfgr CHAR(25),p_brand CHAR(10),p_type VARCHAR(25),p_size INT,p_container CHAR(10),p_retailprice DECIMAL,p_comment VARCHAR(23)) USING csv OPTIONS(path './tpch/tpch_table/part.tbl', delimiter '|')")
spark.sql("CREATE TABLE CUSTOMER (c_custkey INT,c_name VARCHAR(25),c_address VARCHAR(40),c_nationkey INT,c_phone CHAR(15),c_acctbal DECIMAL,c_mktsegment CHAR(10),c_comment VARCHAR(117)) USING csv OPTIONS(path './tpch/tpch_table/customer.tbl', delimiter '|')")
spark.sql("CREATE TABLE SUPPLIER (s_suppkey INT,s_name CHAR(25),s_address VARCHAR(40),s_nationkey INT,s_phone CHAR(15),s_acctbal DECIMAL,s_comment VARCHAR(101)) USING csv OPTIONS(path './tpch/tpch_table/supplier.tbl', delimiter '|')")
spark.sql("CREATE TABLE PARTSUPP (ps_partkey INT,ps_suppkey INT,ps_availqty INT,ps_supplycost DECIMAL,ps_comment VARCHAR(199)) USING csv OPTIONS(path './tpch/tpch_table/partsupp.tbl', delimiter '|')")
spark.sql("CREATE TABLE NATION (n_nationkey INT,n_name CHAR(25),n_regionkey INT,n_comment VARCHAR(152)) USING csv OPTIONS(path './tpch/tpch_table/nation.tbl', delimiter '|')")
spark.sql("CREATE TABLE REGION (r_regionkey INT,r_name CHAR(25),r_comment VARCHAR(152)) USING csv OPTIONS(path './tpch/tpch_table/region.tbl', delimiter '|')")
spark.sql("select 	s_name, 	s_address from 	supplier, 	nation where 	s_suppkey in ( 		select 			ps_suppkey 		from 			partsupp 		where 			ps_partkey in ( 				select 					p_partkey 				from 					part 				where 					p_name like 'forest%' 			) 			and ps_availqty > ( 				select 					0.5 * sum(l_quantity) 				from 					lineitem 				where 					l_partkey = ps_partkey 					and l_suppkey = ps_suppkey 					and l_shipdate >= date '1994-01-01' 					and l_shipdate < date '1994-01-01' + interval '1' year 			) 	) 	and s_nationkey = n_nationkey 	and n_name = 'CANADA' order by 	s_name").explain(extended=true)

System.exit(0)