import argparse

from gm_params import *
from gm_sql import *
from math import sqrt
import os
import time

db_conn = None;

def iterRankOne():
	A="a"
	B="b"
	C="c"
	cur=db_conn.cursor()
	#update
	cur.execute("select big.src_id as src_id, sum(big.score*b.score*c.score) as score into tempa from %s as big, b, c where big.dst_id=b.dst_id and big.port=c.port group by big.src_id" % GM_TABLE);
	cur.execute("select big.dst_id as dst_id, sum(big.score*a.score*c.score) as score into tempb from %s as big, a, c where big.src_id=a.src_id and big.port=c.port group by big.dst_id" % GM_TABLE);
	cur.execute("select big.port as port, sum(big.score*a.score*b.score) as score into tempc from %s as big, a, b where big.src_id=a.src_id and big.dst_id=b.dst_id group by big.port" % GM_TABLE);
	#clean up
	cur.execute("drop table a")
	cur.execute("drop table b")
	cur.execute("drop table c")
	cur.execute("select * into a from tempa")
	cur.execute("select * into b from tempb")
	cur.execute("select * into c from tempc")
	cur.execute("drop table tempa")
	cur.execute("drop table tempb")
	cur.execute("drop table tempc")	
	#prevent underflow
	cur.execute("update a set score=0 where score<1*1.0e-20")
	cur.execute("update b set score=0 where score<1*1.0e-20")
	cur.execute("update c set score=0 where score<1*1.0e-20")
	#normalize
	cur.execute("Select max(score) from a")
	num=cur.fetchone()[0]
	cur.execute("update a set score=score/%s" % str(num))
	cur.execute("Select max(score) from b")
	num=cur.fetchone()[0]
	cur.execute("update b set score=score/%s" % str(num))
	cur.execute("Select max(score) from c")
	num=cur.fetchone()[0]
	cur.execute("update c set score=score/%s" % str(num))
	
def tensorInit():
	A="a"
	B="b"
	C="c"
	gm_sql_table_drop_create(db_conn, A, "src_id integer, score double precision")
	gm_sql_table_drop_create(db_conn, B, "dst_id integer, score double precision")
	gm_sql_table_drop_create(db_conn, C, "port integer, score double precision")
	cur=db_conn.cursor()
	cur.execute("insert into a select distinct src_id, 1 as score from %s" % GM_TABLE)
	cur.execute("insert into b select distinct dst_id, 1 as score from %s" % GM_TABLE)
	cur.execute("insert into c select distinct port, 1 as score from %s" % GM_TABLE)

def main():
    global db_conn
    global GM_TABLE
     # Command Line processing
    parser = argparse.ArgumentParser(description="Tensor Using SQL v1.0")
    parser.add_argument ('--file', dest='input_file', type=str, required=True,
                         help='Full path to the file to load from. For weighted \
                         graphs, the file should have the format (<src_id>, <dst_id>, <weight>) \
                         . If unweighted please run with --unweighted option. To specify a \
                         delimiter other than "," (default), use --delim option. \
                         NOTE: The file should have proper permissions set for \
                         the postgres user.' 
                         )
    parser.add_argument ('--delim', dest='delimiter', type=str, default='\t',
                         help='Delimiter that separate the columns in the input file. default ","')
                        
    parser.add_argument ('--dest_dir', dest='dest_dir', type=str, required=True,
                         help='Full path to the directory where the output tables are saved')
                         
                         
    args = parser.parse_args()
    
    try:
        # Run the various graph algorithm below
        db_conn = gm_db_initialize()
        
        gm_sql_table_drop_create(db_conn, GM_TABLE, "src_id integer, dst_id integer, port integer, score integer")
        col_fmt = "src_id, dst_id, port, score"
            
        gm_sql_load_table_from_file(db_conn, GM_TABLE, col_fmt, args.input_file, args.delimiter)
        tensorInit()
        for i in xrange(20):
            iterRankOne()
        gm_sql_save_table_to_file(db_conn, 'a',"src_id, score",\
                                   os.path.join(args.dest_dir,"a.csv"),",");
 
        gm_sql_save_table_to_file(db_conn, 'b',"dst_id, score",\
                                   os.path.join(args.dest_dir,"b.csv"),",");
                   
        gm_sql_save_table_to_file(db_conn, 'c',"port, score",\
                                   os.path.join(args.dest_dir,"c.csv"),",");
        
        # Save tables to disk
                     
        gm_db_bubye(db_conn)
    except:
        print "Unexpected error:", sys.exc_info()[0]    
        if (db_conn):
            gm_db_bubye(db_conn)
            
        raise                    

if __name__ == '__main__':
    main()
