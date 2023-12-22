<div align="center">
<p align="center">

<img width="275" alt="Epsilla Logo" src="https://epsilla-misc.s3.amazonaws.com/epsilla-horizontal.png">

**A 10x faster, cheaper, and better vector database**

<a href="https://epsilla-inc.gitbook.io/epsilladb/">Documentation</a> •
<a href="https://discord.gg/cDaY2CxZc5">Discord</a> •
<a href="https://twitter.com/epsilla_inc">Twitter</a> •
<a href="https://blog.epsilla.com">Blog</a> •
<a href="https://www.youtube.com/@Epsilla-kp5cx">YouTube</a> •
<a href="https://forms.gle/z73ra1sGBxH9wiUR8">Feedback</a>

</div>

<hr />

Epsilla is an open-source vector database. Our focus is on ensuring scalability, high performance, and cost-effectiveness of vector search. EpsillaDB bridges the gap between information retrieval and memory retention in Large Language Models.

The key features of Epsilla include:

* High performance and production-scale similarity search for embedding vectors.

* Full fledged database management system with familiar database, table, and field concepts. Vector is just another field type.

* Metadata filtering.

* Hybrid search with a fusion of dense and sparse vectors.

* Cloud native architecture with compute storage separation, serverless, and multi-tenancy.

* Rich ecosystem integrations including LangChain and LlamaIndex.

* Python/JavaScript/Ruby clients, and REST API interface.

Epsilla's core is written in C++ and leverages the advanced academic parallel graph traversal techniques for vector indexing, achieving 10 times faster vector search than HNSW while maintaining precision levels of over 99.9%.

## Quick Start with Epsilla Vector Database in Docker

**1. Run Backend in Docker**
```shell
docker pull epsilla/vectordb
docker run --pull=always -d -p 8888:8888 -v /tmp:/tmp epsilla/vectordb
```

**2. Interact with Python Client**
```shell
pip install pyepsilla
```

```python
from pyepsilla import vectordb

client = vectordb.Client(host='localhost', port='8888')
client.load_db(db_name="MyDB", db_path="/tmp/epsilla")
client.use_db(db_name="MyDB")

client.create_table(
    table_name="MyTable",
    table_fields=[
        {"name": "ID", "dataType": "INT", "primaryKey": True},
        {"name": "Doc", "dataType": "STRING"},
    ],
    indices=[
      {"name": "Index", "field": "Doc"},
    ]
)

client.insert(
    table_name="MyTable",
    records=[
        {"ID": 1, "Doc": "Jupiter is the largest planet in our solar system."},
        {"ID": 2, "Doc": "Cheetahs are the fastest land animals, reaching speeds over 60 mph."},
        {"ID": 3, "Doc": "Vincent van Gogh painted the famous work \"Starry Night.\""},
        {"ID": 4, "Doc": "The Amazon River is the longest river in the world."},
        {"ID": 5, "Doc": "The Moon completes one orbit around Earth every 27 days."},
    ],
)

client.query(
    table_name="MyTable",
    query_text="Celestial bodies and their characteristics",
    limit=2
)

# Result
# {
#     'message': 'Query search successfully.',
#     'result': [
#         {'Doc': 'Jupiter is the largest planet in our solar system.', 'ID': 1},
#         {'Doc': 'The Moon completes one orbit around Earth every 27 days.', 'ID': 5}
#     ],
#     'statusCode': 200
# }
```

## Epsilla Cloud

Try our fully managed vector DBaaS at <a href="https://cloud.epsilla.com/">Epsilla Cloud</a>

## Quick Start with Epsilla Python Bindings Lib, without launching epsilla vector database

**1. Build Epsilla Python Bindings lib package**
```shell
cd engine
bash build.sh
ls -lh build/*.so
```

**2. Run test with python bindings lib "epsilla.so" "libvectordb_dylib.so in the folder "build" built in the previous step**
```shell
cd engine
export PYTHONPATH=./build/
export DB_PATH=/tmp/db33
python3 test/bindings/python/test.py
```

