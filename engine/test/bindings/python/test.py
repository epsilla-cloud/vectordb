import epsilla
import os
test_db_path = os.environ.get("DB_PATH")
a = epsilla.load_db(db_name="db", db_path=test_db_path)
epsilla.use_db(db_name="db")
epsilla.create_table(
    table_name="MyTable",
    table_fields=[
        {"name": "ID", "dataType": "INT", "primaryKey": True},
        {"name": "Doc", "dataType": "STRING"},
        {"name": "EmbeddingEuclidean", "dataType": "VECTOR_FLOAT",
            "dimensions": 4, "metricType": "EUCLIDEAN"},
        {"name": "EmbeddingDotProduct", "dataType": "VECTOR_FLOAT",
            "dimensions": 4, "metricType": "DOT_PRODUCT"},
        {"name": "EmbeddingCosine", "dataType": "VECTOR_FLOAT",
            "dimensions": 4, "metricType": "COSINE"},
    ]
)

epsilla.insert(
    table_name="MyTable",
    records=[
        {
            "ID": 1,
            "Doc": "Berlin",
            "EmbeddingEuclidean": [0.05, 0.61, 0.76, 0.74],
            "EmbeddingDotProduct": [0.05, 0.61, 0.76, 0.74],
            "EmbeddingCosine": [0.05, 0.61, 0.76, 0.74]
        },
        {
            "ID": 2,
            "Doc": "London",
            "EmbeddingEuclidean": [0.19, 0.81, 0.75, 0.11],
            "EmbeddingDotProduct": [0.19, 0.81, 0.75, 0.11],
            "EmbeddingCosine": [0.19, 0.81, 0.75, 0.11]
        },
        {
            "ID": 3,
            "Doc": "Moscow",
            "EmbeddingEuclidean": [0.36, 0.55, 0.47, 0.94],
            "EmbeddingDotProduct": [0.36, 0.55, 0.47, 0.94],
            "EmbeddingCosine": [0.36, 0.55, 0.47, 0.94]
        },
        {
            "ID": 4,
            "Doc": "San Francisco",
            "EmbeddingEuclidean": [0.18, 0.01, 0.85, 0.80],
            "EmbeddingDotProduct": [0.18, 0.01, 0.85, 0.80],
            "EmbeddingCosine": [0.18, 0.01, 0.85, 0.80]
        },
        {
            "ID": 5,
            "Doc": "Shanghai",
            "EmbeddingEuclidean": [0.24, 0.18, 0.22, 0.44],
            "EmbeddingDotProduct": [0.24, 0.18, 0.22, 0.44],
            "EmbeddingCosine": [0.24, 0.18, 0.22, 0.44]
        },
        # duplicate insertion
        {
            "ID": 1,
            "Doc": "Berlin",
            "EmbeddingEuclidean": [0.05, 0.61, 0.76, 0.74],
            "EmbeddingDotProduct": [0.05, 0.61, 0.76, 0.74],
            "EmbeddingCosine": [0.05, 0.61, 0.76, 0.74]
        },
    ]
)

for field in ["EmbeddingEuclidean", "EmbeddingDotProduct", "EmbeddingCosine"]:
    (code, response) = epsilla.query(
        table_name="MyTable",
        query_field="EmbeddingEuclidean",
        response_fields=["ID", "Doc", field],
        query_vector=[0.35, 0.55, 0.47, 0.94],
        limit=2,
        with_distance=True
    )

    print("searching on {}: code {} response {}".format(field, code, response))


pk_to_delete = [1, 2, 3, 4]
print("deleting pk ", pk_to_delete)
code = epsilla.delete(table_name="MyTable", primary_keys=pk_to_delete)
print("delete return code:", code)

(code, response) = epsilla.query(
    table_name="MyTable",
    query_field="EmbeddingEuclidean",
    response_fields=["ID", "Doc", "EmbeddingEuclidean"],
    query_vector=[0.35, 0.55, 0.47, 0.94],
    limit=10,
    with_distance=True
)
print(code, response)
epsilla.drop_table("MyTable")

epsilla.unload_db("MyDB")
