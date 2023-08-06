import epsilla
a = epsilla.load_db("db", "/tmp/db2")
epsilla.use_db("db")
epsilla.create_table("MyTable", [ {"name": "ID", "dataType": "INT"}, {"name": "Doc", "dataType": "STRING"}, {"name": "Embedding", "dataType": "VECTOR_FLOAT", "dimensions": 4} ] )
