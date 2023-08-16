import threading
from pyepsilla import vectordb

# connect to vectordb
client = vectordb.Client(
    host='localhost',
    port='8888'
)

# load and use a database
client.load_db(db_name="MyDB", db_path="/tmp/epsilla")
client.use_db(db_name="MyDB")

# create a table in the current database
client.create_table(
    table_name="MyTable",
    table_fields=[
        {"name": "ID", "dataType": "INT"},
        {"name": "Doc", "dataType": "STRING"},
        {"name": "Embedding", "dataType": "VECTOR_FLOAT", "dimensions": 4}
    ]
)

# insert records
client.insert(
    table_name="MyTable",
    records=[
        {"ID": 1, "Doc": "Berlin", "Embedding": [0.05, 0.61, 0.76, 0.74]},
        {"ID": 2, "Doc": "London", "Embedding": [0.19, 0.81, 0.75, 0.11]},
        {"ID": 3, "Doc": "Moscow", "Embedding": [0.36, 0.55, 0.47, 0.94]},
        {"ID": 4, "Doc": "San Francisco",
            "Embedding": [0.18, 0.01, 0.85, 0.80]},
        {"ID": 5, "Doc": "Shanghai", "Embedding": [0.24, 0.18, 0.22, 0.44]}
    ]
)

# search
for i in range(10):
    status_code, response = client.query(
        table_name="MyTable",
        query_field="Embedding",
        query_vector=[0.35, 0.55, 0.47, 0.94],
        limit=2
    )
    print(response)


def run_task():
    (code, response) = client.query(
        table_name="MyTable",
        query_field="Embedding",
        query_vector=[0.35, 0.55, 0.47, 0.94],
        limit=2
    )
    print(code, response)


for _ in range(10):
    threads = [threading.Thread(target=run_task) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("concurrency test done")

# drop a table
client.drop_table("MyTable")

# unload a database from memory
client.unload_db("MyDB")
