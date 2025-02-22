import duckdb

conn = duckdb.connect(database="data/staging/data.duckdb")
conn.execute("drop table trips;")
