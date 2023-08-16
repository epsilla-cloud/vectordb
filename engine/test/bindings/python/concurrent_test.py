import threading
import epsilla
import os
test_db_path = os.environ.get("DB_PATH")
a = epsilla.load_db(db_name="db", db_path=test_db_path)
epsilla.use_db(db_name="db")
epsilla.create_table(
    table_name="MyTable",
    table_fields=[
        {"name": "ID", "dataType": "INT"},
        {"name": "Doc", "dataType": "STRING"},
        {"name": "Embedding", "dataType": "VECTOR_FLOAT", "dimensions": 4}
    ]
)

epsilla.insert(
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


def run_task():
    (code, response) = epsilla.query(
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

epsilla.drop_table("MyTable")

epsilla.unload_db("MyDB")
