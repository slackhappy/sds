import sqlite3
import sys
import wikipedia_views_pb2
import os

os.unlink('dbs/test.db')
con = sqlite3.connect('dbs/test.db')
cur = con.execute("CREATE TABLE metadata(key, value)")
metadata_list = [
    ('name', 'test'),
    ("total_shards", '1'),
    ("shard", '0'),
    ("owner", 'John'),
    ("created_ts", 'John'),
    ("ValueType", 'proto'),
    ("ValueClass", 'io.github.slackhappy.sds.examples.Test'),
]
cur.executemany("insert into metadata values (?, ?)", metadata_list)
cur = con.execute("CREATE TABLE data(key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID")

#https://dumps.wikimedia.org/other/pageviews/2024/2024-06/pageviews-20240601-000000.gz

for line in open(sys.argv[1], 'r'):
    (domain, title, viewcount, resp_size) = line.split(' ')
    v = wikipedia_views_pb2.Views(count=int(viewcount))
    cur.execute("insert into data values (?, ?)", (f"{title}|{domain}", v.SerializeToString()))
con.commit()
con.close()