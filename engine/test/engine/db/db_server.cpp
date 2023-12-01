
#include "db/db_server.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>  // std::chrono::seconds
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <thread>  // std::this_thread::sleep_for
#include <vector>

TEST(DbServer, CreateTable) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_create_table");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  auto loadDbStatus = database.LoadDB(dbName, tempDir, 150000, true);
  EXPECT_TRUE(loadDbStatus.ok()) << "message:" << loadDbStatus.message();

  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT",
      "primaryKey": true
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingDotProduct",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingCosine",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "COSINE"
    },
    {
      "name": "EmbeddingSparseEuclidean",
      "dataType": "SPARSE_VECTOR_DOUBLE",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingSparseDotProduct",
      "dataType": "SPARSE_VECTOR_DOUBLE",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingSparseCosine",
      "dataType": "SPARSE_VECTOR_DOUBLE",
      "dimensions": 4,
      "metricType": "COSINE"
    }
  ]
}
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << "message:" << createTableStatus.message();
  auto dropTableStatus = database.DropTable(dbName, "MyTable");
  EXPECT_TRUE(dropTableStatus.ok()) << "message:" << dropTableStatus.message();
  auto unloadDbStatus = database.UnloadDB(dbName);
  EXPECT_TRUE(unloadDbStatus.ok()) << "message:" << unloadDbStatus.message();
}

TEST(DbServer, DenseVector) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_dense_vector");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  const auto tableName = "MyTable";
  size_t queryDimension = 4;
  database.LoadDB(dbName, tempDir, 150000, true);
  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT",
      "primaryKey": true
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingDotProduct",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingCosine",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "COSINE"
    }
  ]
}
    )_";

  const std::string records = R"_(
[
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingDotProduct": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingCosine": [
      0.05,
      0.61,
      0.76,
      0.74
    ]
  },
  {
    "ID": 2,
    "Doc": "London",
    "EmbeddingEuclidean": [
      0.19,
      0.81,
      0.75,
      0.11
    ],
    "EmbeddingDotProduct": [
      0.19,
      0.81,
      0.75,
      0.11
    ],
    "EmbeddingCosine": [
      0.19,
      0.81,
      0.75,
      0.11
    ]
  },
  {
    "ID": 3,
    "Doc": "Moscow",
    "EmbeddingEuclidean": [
      0.36,
      0.55,
      0.47,
      0.94
    ],
    "EmbeddingDotProduct": [
      0.36,
      0.55,
      0.47,
      0.94
    ],
    "EmbeddingCosine": [
      0.36,
      0.55,
      0.47,
      0.94
    ]
  },
  {
    "ID": 4,
    "Doc": "San Francisco",
    "EmbeddingEuclidean": [
      0.18,
      0.01,
      0.85,
      0.8
    ],
    "EmbeddingDotProduct": [
      0.18,
      0.01,
      0.85,
      0.8
    ],
    "EmbeddingCosine": [
      0.18,
      0.01,
      0.85,
      0.8
    ]
  },
  {
    "ID": 5,
    "Doc": "Shanghai",
    "EmbeddingEuclidean": [
      0.24,
      0.18,
      0.22,
      0.44
    ],
    "EmbeddingDotProduct": [
      0.24,
      0.18,
      0.22,
      0.44
    ],
    "EmbeddingCosine": [
      0.24,
      0.18,
      0.22,
      0.44
    ]
  },
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingDotProduct": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingCosine": [
      0.05,
      0.61,
      0.76,
      0.74
    ]
  }
]
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json recordsJson;
  EXPECT_TRUE(recordsJson.LoadFromString(records));
  auto insertStatus = database.Insert(dbName, tableName, recordsJson);
  EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  vectordb::engine::DenseVectorElement queryData[] = {0.35, 0.55, 0.47, 0.94};
  auto badQueryDataPtr = std::make_shared<vectordb::engine::SparseVector>(
      vectordb::engine::SparseVector({{0, 0.35}, {1, 0.55}, {2, 0.47}, {3, 0.94}}));

  struct TestCase {
    std::string searchFieldName;
    std::vector<std::string> expectedOrder;
  };
  std::vector<TestCase> testcases = {
      {"EmbeddingEuclidean", {"Moscow", "Berlin", "Shanghai", "San Francisco", "London"}},
      {"EmbeddingDotProduct", {"Moscow", "Berlin", "San Francisco", "London", "Shanghai"}},
      {"EmbeddingCosine", {"Moscow", "Shanghai", "Berlin", "San Francisco", "London"}}};
  auto rebuildOptions = {false, true};
  for (auto rebuild : rebuildOptions) {
    if (rebuild) {
      auto rebuildStatus = database.Rebuild();
      EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
    }
    for (auto &testcase : testcases) {
      vectordb::Json result;
      const auto limit = 6;
      auto queryFields = std::vector<std::string>{"ID", "Doc", testcase.searchFieldName};
      auto queryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, queryData, limit, result, "", true);
      EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
      EXPECT_EQ(result.GetSize(), 5) << "duplicate insert should've been ignored";
      for (int i = 0; i < result.GetSize(); i++) {
        EXPECT_EQ(result.GetArrayElement(i).GetString("Doc"), testcase.expectedOrder[i])
            << i << "th city mismatch when querying " << testcase.searchFieldName << std::endl
            << result.DumpToString();
      }

      auto badQueryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, badQueryDataPtr, limit, result, "", true);
      EXPECT_FALSE(badQueryStatus.ok()) << "query dense vector with sparse vector should fail";
    }
  }
}

TEST(DbServer, SparseVector) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_sparse_vector");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  const auto tableName = "MyTable";
  size_t queryDimension = 4;
  database.LoadDB(dbName, tempDir, 150000, true);
  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT",
      "primaryKey": true
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingDotProduct",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingCosine",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "COSINE"
    }
  ]
}
    )_";

  const std::string records = R"_(
[
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    }
  },
  {
    "ID": 2,
    "Doc": "London",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    }
  },
  {
    "ID": 3,
    "Doc": "Moscow",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    }
  },
  {
    "ID": 4,
    "Doc": "San Francisco",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    }
  },
  {
    "ID": 5,
    "Doc": "Shanghai",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    }
  },
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    }
  }
]
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json recordsJson;
  EXPECT_TRUE(recordsJson.LoadFromString(records));
  auto insertStatus = database.Insert(dbName, tableName, recordsJson);
  EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  auto queryDataPtr = std::make_shared<vectordb::engine::SparseVector>(
      vectordb::engine::SparseVector({{0, 0.35}, {1, 0.55}, {2, 0.47}, {3, 0.94}}));
  vectordb::engine::DenseVectorElement badQueryDataPtr[] = {0.35, 0.55, 0.47, 0.94};
  struct TestCase {
    std::string searchFieldName;
    std::vector<std::string> expectedOrder;
  };
  std::vector<TestCase> testcases = {
      {"EmbeddingEuclidean", {"Moscow", "Berlin", "Shanghai", "San Francisco", "London"}},
      {"EmbeddingDotProduct", {"Moscow", "Berlin", "San Francisco", "London", "Shanghai"}},
      {"EmbeddingCosine", {"Moscow", "Shanghai", "Berlin", "San Francisco", "London"}}};
  auto rebuildOptions = {false, true};
  for (auto rebuild : rebuildOptions) {
    if (rebuild) {
      auto rebuildStatus = database.Rebuild();
      EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
    }
    for (auto &testcase : testcases) {
      std::cerr << "testcase: " << testcase.searchFieldName << std::endl;
      vectordb::Json result;
      const auto limit = 6;
      auto queryFields = std::vector<std::string>{"ID", "Doc", testcase.searchFieldName};
      auto queryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, queryDataPtr, limit, result, "", true);
      EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
      EXPECT_EQ(result.GetSize(), 5) << "duplicate insert should've been ignored";
      for (int i = 0; i < result.GetSize(); i++) {
        EXPECT_EQ(result.GetArrayElement(i).GetString("Doc"), testcase.expectedOrder[i])
            << i << "th city mismatch when querying " << testcase.searchFieldName << std::endl
            << result.DumpToString();
      }

      auto badQueryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, badQueryDataPtr, limit, result, "", true);
      EXPECT_FALSE(badQueryStatus.ok()) << "query sparse vector column with dense vector should fail";
    }
  }
}

TEST(DbServer, DeleteByPK) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_delete_by_pk");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  const auto tableName = "MyTable";
  size_t queryDimension = 4;
  database.LoadDB(dbName, tempDir, 150000, true);
  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT",
      "primaryKey": true
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingDotProduct",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingCosine",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "COSINE"
    }
  ]
}
    )_";

  const std::string records = R"_(
[
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingDotProduct": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingCosine": [
      0.05,
      0.61,
      0.76,
      0.74
    ]
  },
  {
    "ID": 2,
    "Doc": "London",
    "EmbeddingEuclidean": [
      0.19,
      0.81,
      0.75,
      0.11
    ],
    "EmbeddingDotProduct": [
      0.19,
      0.81,
      0.75,
      0.11
    ],
    "EmbeddingCosine": [
      0.19,
      0.81,
      0.75,
      0.11
    ]
  },
  {
    "ID": 3,
    "Doc": "Moscow",
    "EmbeddingEuclidean": [
      0.36,
      0.55,
      0.47,
      0.94
    ],
    "EmbeddingDotProduct": [
      0.36,
      0.55,
      0.47,
      0.94
    ],
    "EmbeddingCosine": [
      0.36,
      0.55,
      0.47,
      0.94
    ]
  },
  {
    "ID": 4,
    "Doc": "San Francisco",
    "EmbeddingEuclidean": [
      0.18,
      0.01,
      0.85,
      0.8
    ],
    "EmbeddingDotProduct": [
      0.18,
      0.01,
      0.85,
      0.8
    ],
    "EmbeddingCosine": [
      0.18,
      0.01,
      0.85,
      0.8
    ]
  },
  {
    "ID": 5,
    "Doc": "Shanghai",
    "EmbeddingEuclidean": [
      0.24,
      0.18,
      0.22,
      0.44
    ],
    "EmbeddingDotProduct": [
      0.24,
      0.18,
      0.22,
      0.44
    ],
    "EmbeddingCosine": [
      0.24,
      0.18,
      0.22,
      0.44
    ]
  },
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingDotProduct": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingCosine": [
      0.05,
      0.61,
      0.76,
      0.74
    ]
  }
]
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json recordsJson;
  EXPECT_TRUE(recordsJson.LoadFromString(records));
  auto insertStatus = database.Insert(dbName, tableName, recordsJson);
  EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  vectordb::engine::DenseVectorElement queryData[] = {0.35, 0.55, 0.47, 0.94};
  struct TestCase {
    std::string searchFieldName;
    std::vector<std::string> expectedOrder;
  };
  std::vector<TestCase> testcases = {
      {"EmbeddingEuclidean", {"Moscow", "Berlin", "Shanghai", "San Francisco", "London"}},
      {"EmbeddingDotProduct", {"Moscow", "Berlin", "San Francisco", "London", "Shanghai"}},
      {"EmbeddingCosine", {"Moscow", "Shanghai", "Berlin", "San Francisco", "London"}}};
  for (auto &testcase : testcases) {
    vectordb::Json result;
    const auto limit = 6;
    auto queryFields = std::vector<std::string>{"ID", "Doc", testcase.searchFieldName};
    auto queryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, queryData, limit, result, "", true);
    EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
    EXPECT_EQ(result.GetSize(), 5) << "duplicate insert should've been ignored";
    for (int i = 0; i < result.GetSize(); i++) {
      EXPECT_EQ(result.GetArrayElement(i).GetString("Doc"), testcase.expectedOrder[i])
          << i << "th city mismatch when querying " << testcase.searchFieldName << std::endl
          << result.DumpToString();
    }
  }

  vectordb::Json pksToDel;
  pksToDel.LoadFromString("[]");
  for (int i = 1; i <= 4; i++) {
    pksToDel.AddIntToArray(i);
  }
  database.Delete(dbName, tableName, pksToDel, "");
  for (auto &testcase : testcases) {
    vectordb::Json result;
    const auto limit = 6;
    auto queryFields = std::vector<std::string>{"ID", "Doc", testcase.searchFieldName};
    auto queryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, queryData, limit, result, "", true);
    EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
    EXPECT_EQ(result.GetSize(), 1) << "records with ID 1/2/3/4 should've been deleted";
    for (int i = 0; i < result.GetSize(); i++) {
      EXPECT_EQ(result.GetArrayElement(i).GetString("Doc"), "Shanghai")
          << "deleted entry returned as result"
          << result.DumpToString();
    }
  }
}

TEST(DbServer, RebuildDenseVector) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_rebuild_dense_vector");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  size_t tableId = 0;
  auto dbName = "MyDB";
  vectordb::Json schemaObj;

  // read schema
  std::ifstream schemaStream("engine/db/testdata/dense_schema_1.json");
  std::string schema;
  schemaStream.seekg(0, std::ios::end);
  schema.reserve(schemaStream.tellg());
  schemaStream.seekg(0, std::ios::beg);
  schema.assign((std::istreambuf_iterator<char>(schemaStream)),
                std::istreambuf_iterator<char>());
  schemaObj.LoadFromString(schema);
  auto tableName = schemaObj.GetString("name");

  database.LoadDB(dbName, tempDir, 150000, true);

  std::ifstream recordStream("engine/db/testdata/dense_data_1.json");
  std::string records;
  recordStream.seekg(0, std::ios::end);
  records.reserve(recordStream.tellg());
  recordStream.seekg(0, std::ios::beg);
  records.assign((std::istreambuf_iterator<char>(recordStream)),
                 std::istreambuf_iterator<char>());

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json dataObj;
  dataObj.LoadFromString(records);
  vectordb::Json recordsJson = dataObj.Get("data");
  EXPECT_GT(recordsJson.GetSize(), 0) << recordsJson.DumpToString();
  for (int i = 0; i < 20; i++) {
    auto insertStatus = database.Insert(dbName, tableName, recordsJson);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // check rebuild
  auto rebuildStatus = database.Rebuild();
  EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
}

vectordb::Json objectArrayToJson(std::vector<vectordb::Json> c) {
  vectordb::Json result;
  result.LoadFromString("[]");
  for (const auto &item : c) {
    result.AddObjectToArray(item);
  }
  return result;
}

TEST(DbServer, QueryDenseVectorDuringRebuild) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_query_dense_vector_during_rebuild");
  vectordb::engine::DBServer database;
  database.SetLeader(true);
  std::filesystem::remove_all(tempDir);
  size_t tableId = 0;
  auto dbName = "MyDB";
  vectordb::Json schemaObj;
  const auto limit = 500;
  const auto dimension = 2;
  const auto numRecords = 10000;
  const auto initialRecords = 300;
  vectordb::engine::DenseVectorElement queryData[] = {1.0, 0.0};
  auto queryFields = std::vector<std::string>{"ID", "Vec"};
  std::string fieldName = "Vec";
  vectordb::Json queryResult;

  std::string schema = R"_(
{
    "name": "PartialRebuild",
    "returnTableId": true,
    "fields": [
        {
            "name": "ID",
            "dataType": "Int",
            "primaryKey": true
        },
        {
            "name": "Theta",
            "dataType": "String"
        },
        {
            "name": "Vec",
            "dataType": "VECTOR_FLOAT",
            "metricType": "COSINE",
            "dimensions": 2
        }
    ]
}
  )_";
  schemaObj.LoadFromString(schema);
  auto tableName = schemaObj.GetString("name");

  database.LoadDB(dbName, tempDir, numRecords, true);
  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();

  auto records = std::vector<vectordb::Json>();
  for (int i = 0; i < numRecords; i++) {
    vectordb::Json item, vec;
    vec.LoadFromString("[]");
    auto x = std::cos(M_PI / numRecords * i);
    auto y = std::sin(M_PI / numRecords * i);
    vec.AddDoubleToArray(x);
    vec.AddDoubleToArray(y);
    item.LoadFromString("{}");
    item.SetInt("ID", i);
    item.SetObject("Vec", vec);
    item.SetString("Theta", std::to_string(i) + "/" + std::to_string(numRecords) + " * PI");
    records.push_back(item);
  }
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(records.begin(), records.end(), g);
  auto itemsFirstHalf = std::vector<vectordb::Json>(records.begin(), records.begin() + records.size() / 2);
  auto itemsSecondHalf = std::vector<vectordb::Json>(records.begin() + records.size() / 2, records.end());
  auto itemFirstHalfJson = objectArrayToJson(itemsFirstHalf);
  auto itemSecondHalfJson = objectArrayToJson(itemsSecondHalf);
  std::sort(itemsFirstHalf.begin(), itemsFirstHalf.end(), [](vectordb::Json a, vectordb::Json b) {
    return a.GetInt("ID") < b.GetInt("ID");
  });
  std::sort(itemsSecondHalf.begin(), itemsSecondHalf.end(), [](vectordb::Json a, vectordb::Json b) {
    return a.GetInt("ID") < b.GetInt("ID");
  });

  // insert first half of records and query top 500 in fully rebuilt 5000 records
  {
    auto insertStatus = database.Insert(dbName, tableName, itemFirstHalfJson);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();

    auto rebuildStatus = database.Rebuild();
    EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
    auto queryStatus = database.Search(dbName, tableName, fieldName, queryFields, dimension, queryData, limit, queryResult, "", true);
    EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
    EXPECT_EQ(queryResult.GetSize(), limit) << "more than expected entries loaded";
    for (int i = 0; i < queryResult.GetSize(); i++) {
      EXPECT_EQ(queryResult.GetArrayElement(i).GetInt("ID"), itemsFirstHalf[i].GetInt("ID"))
          << "incorrect return order"
          << std::endl
          << queryResult.DumpToString()
          << std::endl
          << "expecting"
          << objectArrayToJson(itemsFirstHalf).DumpToString();
    }
  }

  // insert the remaining half of records and query top 500 in 5000 prebuilt records + 5000 non-rebuilt records
  {
    auto insertStatus = database.Insert(dbName, tableName, itemSecondHalfJson);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();

    // verify query result before rebuild
    auto preRebuildQueryStatus = database.Search(dbName, tableName, fieldName, queryFields, dimension, queryData, limit, queryResult, "", true);
    EXPECT_TRUE(preRebuildQueryStatus.ok()) << preRebuildQueryStatus.message();
    EXPECT_EQ(queryResult.GetSize(), limit) << "more than expected entries loaded";
    for (int i = 0; i < queryResult.GetSize(); i++) {
      EXPECT_EQ(queryResult.GetArrayElement(i).GetInt("ID"), i) << "incorrect return order"
                                                                << std::endl
                                                                << queryResult.DumpToString();
    }
  }

  // start rebuild asynchronously
  auto rebuildStatusFuture = std::async([&database]() {
    return database.Rebuild();
  });

  // keep querying during rebuild
  size_t queryCountDuringRebuild = 0;
  while (true) {
    auto status = rebuildStatusFuture.wait_for(std::chrono::seconds(0));
    if (status == std::future_status::ready) {
      break;
    }

    // verify query result during rebuild
    auto queryStatus = database.Search(dbName, tableName, fieldName, queryFields, dimension, queryData, limit, queryResult, "", true);
    queryCountDuringRebuild++;
    EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
    EXPECT_EQ(queryResult.GetSize(), limit) << "more than expected entries loaded";
    for (int i = 0; i < queryResult.GetSize(); i++) {
      EXPECT_EQ(queryResult.GetArrayElement(i).GetInt("ID"), i) << "incorrect return order"
                                                                << std::endl
                                                                << queryResult.DumpToString();
    }
  }
  EXPECT_GE(queryCountDuringRebuild, 1) << "at least one query is expected during rebuild";

  // verify query result after rebuild
  auto rebuildStatus = rebuildStatusFuture.get();
  EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
  auto postRebuildQueryStatus = database.Search(dbName, tableName, fieldName, queryFields, dimension, queryData, limit, queryResult, "", true);
  EXPECT_TRUE(postRebuildQueryStatus.ok()) << postRebuildQueryStatus.message();
  EXPECT_EQ(queryResult.GetSize(), limit) << "more than expected entries loaded";
  for (int i = 0; i < queryResult.GetSize(); i++) {
    EXPECT_EQ(queryResult.GetArrayElement(i).GetInt("ID"), i) << "incorrect return order"
                                                              << std::endl
                                                              << queryResult.DumpToString();
  }
}

TEST(DbServer, RebuildSparseVector) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_rebuild_sparse_vector");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  const auto tableName = "MyTable";
  size_t queryDimension = 4;
  database.LoadDB(dbName, tempDir, 150000, true);
  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT"
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingDotProduct",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingCosine",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "COSINE"
    }
  ]
}
    )_";

  const std::string records = R"_(
[
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    }
  },
  {
    "ID": 2,
    "Doc": "London",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    }
  },
  {
    "ID": 3,
    "Doc": "Moscow",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    }
  },
  {
    "ID": 4,
    "Doc": "San Francisco",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    }
  },
  {
    "ID": 5,
    "Doc": "Shanghai",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    }
  },
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    }
  }
]
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json recordsJson;
  EXPECT_TRUE(recordsJson.LoadFromString(records));

  for (int i = 0; i < 20; i++) {
    auto insertStatus = database.Insert(dbName, tableName, recordsJson);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // check rebuild
  auto rebuildStatus = database.Rebuild();
  EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
}

TEST(DbServer, DenseVectorFilter) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_dense_vector");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  const auto tableName = "MyTable";
  size_t queryDimension = 4;
  database.LoadDB(dbName, tempDir, 150000, true);
  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT",
      "primaryKey": true
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingDotProduct",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingCosine",
      "dataType": "VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "COSINE"
    }
  ]
}
    )_";

  const std::string records = R"_(
[
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingDotProduct": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingCosine": [
      0.05,
      0.61,
      0.76,
      0.74
    ]
  },
  {
    "ID": 2,
    "Doc": "London",
    "EmbeddingEuclidean": [
      0.19,
      0.81,
      0.75,
      0.11
    ],
    "EmbeddingDotProduct": [
      0.19,
      0.81,
      0.75,
      0.11
    ],
    "EmbeddingCosine": [
      0.19,
      0.81,
      0.75,
      0.11
    ]
  },
  {
    "ID": 3,
    "Doc": "Moscow",
    "EmbeddingEuclidean": [
      0.36,
      0.55,
      0.47,
      0.94
    ],
    "EmbeddingDotProduct": [
      0.36,
      0.55,
      0.47,
      0.94
    ],
    "EmbeddingCosine": [
      0.36,
      0.55,
      0.47,
      0.94
    ]
  },
  {
    "ID": 4,
    "Doc": "San Francisco",
    "EmbeddingEuclidean": [
      0.18,
      0.01,
      0.85,
      0.8
    ],
    "EmbeddingDotProduct": [
      0.18,
      0.01,
      0.85,
      0.8
    ],
    "EmbeddingCosine": [
      0.18,
      0.01,
      0.85,
      0.8
    ]
  },
  {
    "ID": 5,
    "Doc": "Shanghai",
    "EmbeddingEuclidean": [
      0.24,
      0.18,
      0.22,
      0.44
    ],
    "EmbeddingDotProduct": [
      0.24,
      0.18,
      0.22,
      0.44
    ],
    "EmbeddingCosine": [
      0.24,
      0.18,
      0.22,
      0.44
    ]
  },
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingDotProduct": [
      0.05,
      0.61,
      0.76,
      0.74
    ],
    "EmbeddingCosine": [
      0.05,
      0.61,
      0.76,
      0.74
    ]
  }
]
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json recordsJson;
  EXPECT_TRUE(recordsJson.LoadFromString(records));
  auto insertStatus = database.Insert(dbName, tableName, recordsJson);
  EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  vectordb::engine::DenseVectorElement queryData[] = {0.35, 0.55, 0.47, 0.94};
  struct TestCase {
    std::string searchFieldName;
    std::vector<std::string> expectedOrder;
  };
  std::vector<TestCase> testcases = {
      {"EmbeddingEuclidean", {"Moscow", "Berlin", "Shanghai", "San Francisco", "London"}},
      {"EmbeddingDotProduct", {"Moscow", "Berlin", "San Francisco", "London", "Shanghai"}},
      {"EmbeddingCosine", {"Moscow", "Shanghai", "Berlin", "San Francisco", "London"}}};

  auto rebuildOptions = {false, true};
  for (auto rebuild : rebuildOptions) {
    if (rebuild) {
      auto rebuildStatus = database.Rebuild();
      EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
    }
    // test filter
    for (auto &testcase : testcases) {
      vectordb::Json result;
      const auto limit = 6;
      auto queryFields = std::vector<std::string>{"ID", "Doc", testcase.searchFieldName};
      auto queryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, queryData, limit, result, "ID <= 2", true);
      EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
      EXPECT_EQ(result.GetSize(), 2) << "only ID <= 2 entries should be included";
      for (int i = 0; i < result.GetSize(); i++) {
        EXPECT_LE(result.GetArrayElement(i).GetInt("ID"), 2) << "returned ID larger than 2"
                                                             << std::endl
                                                             << result.DumpToString();
      }
    }
  }
}

TEST(DbServer, SparseVectorFilter) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_sparse_vector");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  const auto tableName = "MyTable";
  size_t queryDimension = 4;
  database.LoadDB(dbName, tempDir, 150000, true);
  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT",
      "primaryKey": true
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    },
    {
      "name": "EmbeddingDotProduct",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "DOT_PRODUCT"
    },
    {
      "name": "EmbeddingCosine",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "COSINE"
    }
  ]
}
    )_";

  const std::string records = R"_(
[
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    }
  },
  {
    "ID": 2,
    "Doc": "London",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    }
  },
  {
    "ID": 3,
    "Doc": "Moscow",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.36, 0.55, 0.47, 0.94]
    }
  },
  {
    "ID": 4,
    "Doc": "San Francisco",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.18, 0.01, 0.85, 0.8]
    }
  },
  {
    "ID": 5,
    "Doc": "Shanghai",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    }
  },
  {
    "ID": 1,
    "Doc": "Berlin",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingDotProduct": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    },
    "EmbeddingCosine": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76, 0.74]
    }
  }
]
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json recordsJson;
  EXPECT_TRUE(recordsJson.LoadFromString(records));
  auto insertStatus = database.Insert(dbName, tableName, recordsJson);
  EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  auto queryDataPtr = std::make_shared<vectordb::engine::SparseVector>(
      vectordb::engine::SparseVector({{0, 0.35}, {1, 0.55}, {2, 0.47}, {3, 0.94}}));

  struct TestCase {
    std::string searchFieldName;
    std::vector<std::string> expectedOrder;
  };
  std::vector<TestCase> testcases = {
      {"EmbeddingEuclidean", {"Moscow", "Berlin", "Shanghai", "San Francisco", "London"}},
      {"EmbeddingDotProduct", {"Moscow", "Berlin", "San Francisco", "London", "Shanghai"}},
      {"EmbeddingCosine", {"Moscow", "Shanghai", "Berlin", "San Francisco", "London"}}};
  auto rebuildOptions = {false, true};
  for (auto rebuild : rebuildOptions) {
    if (rebuild) {
      auto rebuildStatus = database.Rebuild();
      EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
    }
    // test filter
    for (auto &testcase : testcases) {
      std::cerr << "testcase: " << testcase.searchFieldName << std::endl;
      vectordb::Json result;
      const auto limit = 6;
      auto queryFields = std::vector<std::string>{"ID", "Doc", testcase.searchFieldName};
      auto queryStatus = database.Search(dbName, tableName, testcase.searchFieldName, queryFields, queryDimension, queryDataPtr, limit, result, "ID <= 2", true);
      EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
      EXPECT_EQ(result.GetSize(), 2) << "only ID <= 2 entries should be included";
      for (int i = 0; i < result.GetSize(); i++) {
        EXPECT_LE(result.GetArrayElement(i).GetInt("ID"), 2) << "returned ID larger than 2"
                                                             << std::endl
                                                             << result.DumpToString();
      }
    }
  }
}

TEST(DbServer, InsertDenseVectorLargeBatch) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_insert_dense_vector_large_batch");
  vectordb::engine::DBServer database;
  database.SetLeader(true);
  std::filesystem::remove_all(tempDir);
  size_t tableId = 0;
  auto dbName = "MyDB";
  vectordb::Json schemaObj;
  const auto dimension = 2;
  const auto batchSize = 2000;
  const auto numBatches = 40;
  const auto numRecords = batchSize * numBatches;
  vectordb::engine::DenseVectorElement queryData[] = {1, 0};
  auto queryFields = std::vector<std::string>{"ID", "Vec"};
  std::string fieldName = "Vec";
  vectordb::Json queryResult;

  std::string schema = R"_(
{
    "name": "InsertSparseVectorLargeBatch",
    "returnTableId": true,
    "fields": [
        {
            "name": "ID",
            "dataType": "Int",
            "primaryKey": true
        },
        {
            "name": "Theta",
            "dataType": "String"
        },
        {
            "name": "Vec",
            "dataType": "VECTOR_FLOAT",
            "metricType": "COSINE",
            "dimensions": 2
        }
    ]
}
  )_";
  schemaObj.LoadFromString(schema);
  auto tableName = schemaObj.GetString("name");

  database.LoadDB(dbName, tempDir, numRecords, true);
  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();

  auto records = std::vector<vectordb::Json>();
  for (int i = 0; i < numRecords; i++) {
    vectordb::Json item, vec;
    vec.LoadFromString("[]");
    auto x = std::cos(M_PI / 180 * i);
    auto y = std::sin(M_PI / 180 * i);
    vec.AddDoubleToArray(x);
    vec.AddDoubleToArray(y);
    item.LoadFromString("{}");
    item.SetInt("ID", i);
    item.SetObject("Vec", vec);
    item.SetString("Theta", std::to_string(i) + "/ 180 * PI");
    records.push_back(item);
  }

  std::vector<vectordb::Json> recordBatchJson;
  for (int i = 0; i < numBatches; i++) {
    recordBatchJson.push_back(objectArrayToJson(std::vector<vectordb::Json>(records.begin() + i * batchSize, records.begin() + (i + 1) * batchSize)));
  }

  // continuously insert 2 rebuild results
  for (int i = 0; i < numBatches / 2; i++) {
    auto insertStatus = database.Insert(dbName, tableName, recordBatchJson[i]);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // start rebuild asynchronously
  auto rebuildStatusFuture = std::async([&database]() {
    return database.Rebuild();
  });

  // insert during rebuild
  for (int i = numBatches / 2; i < numBatches - 1; i++) {
    auto insertStatus = database.Insert(dbName, tableName, recordBatchJson[i]);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // insert after rebuild is done
  {
    rebuildStatusFuture.wait();
    auto insertStatus = database.Insert(dbName, tableName, recordBatchJson[numBatches - 1]);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // verify query result after rebuild
  auto rebuildStatus = rebuildStatusFuture.get();
  EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
  const auto limit = 100;
  auto postRebuildQueryStatus = database.Search(dbName, tableName, fieldName, queryFields, dimension, queryData, limit, queryResult, "ID < 10", true);
  EXPECT_TRUE(postRebuildQueryStatus.ok()) << postRebuildQueryStatus.message();
  EXPECT_LE(queryResult.GetSize(), 10) << "more than expected entries loaded";
  for (int i = 0; i < queryResult.GetSize(); i++) {
    EXPECT_LE(queryResult.GetArrayElement(i).GetInt("ID"), 10) << "invalid entry returned";
  }
}

TEST(DbServer, InsertSparseVectorLargeBatch) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_insert_sparse_vector_large_batch");
  vectordb::engine::DBServer database;
  database.SetLeader(true);
  std::filesystem::remove_all(tempDir);
  size_t tableId = 0;
  auto dbName = "MyDB";
  vectordb::Json schemaObj;
  const auto dimension = 2;
  const auto batchSize = 2000;
  const auto numBatches = 40;
  const auto numRecords = batchSize * numBatches;
  vectordb::engine::SparseVectorPtr queryData = std::make_shared<vectordb::engine::SparseVector>(vectordb::engine::SparseVector({{0, 1}, {1, 0}}));
  auto queryFields = std::vector<std::string>{"ID", "Vec"};
  std::string fieldName = "Vec";
  vectordb::Json queryResult;

  std::string schema = R"_(
{
    "name": "InsertSparseVectorLargeBatch",
    "returnTableId": true,
    "fields": [
        {
            "name": "ID",
            "dataType": "Int",
            "primaryKey": true
        },
        {
            "name": "Theta",
            "dataType": "String"
        },
        {
            "name": "Vec",
            "dataType": "SPARSE_VECTOR_FLOAT",
            "metricType": "COSINE",
            "dimensions": 2
        }
    ]
}
  )_";
  schemaObj.LoadFromString(schema);
  auto tableName = schemaObj.GetString("name");

  database.LoadDB(dbName, tempDir, numRecords, true);
  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();

  auto records = std::vector<vectordb::Json>();
  for (int i = 0; i < numRecords; i++) {
    vectordb::Json item, vec, indices, values;
    vec.LoadFromString("{}");
    indices.LoadFromString("[0, 1]");
    values.LoadFromString("[]");
    auto x = std::cos(M_PI / 180 * i);
    auto y = std::sin(M_PI / 180 * i);
    values.AddDoubleToArray(x);
    values.AddDoubleToArray(y);
    vec.SetObject("indices", indices);
    vec.SetObject("values", values);
    item.LoadFromString("{}");
    item.SetInt("ID", i);
    item.SetObject("Vec", vec);
    item.SetString("Theta", std::to_string(i) + "/ 180 * PI");
    records.push_back(item);
  }

  std::vector<vectordb::Json> recordBatchJson;
  for (int i = 0; i < numBatches; i++) {
    recordBatchJson.push_back(objectArrayToJson(std::vector<vectordb::Json>(records.begin() + i * batchSize, records.begin() + (i + 1) * batchSize)));
  }

  // continuously insert 2 rebuild results
  for (int i = 0; i < numBatches / 2; i++) {
    auto insertStatus = database.Insert(dbName, tableName, recordBatchJson[i]);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // start rebuild asynchronously
  auto rebuildStatusFuture = std::async([&database]() {
    return database.Rebuild();
  });

  // insert during rebuild
  for (int i = numBatches / 2; i < numBatches - 1; i++) {
    auto insertStatus = database.Insert(dbName, tableName, recordBatchJson[i]);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // insert after rebuild is done
  {
    rebuildStatusFuture.wait();
    auto insertStatus = database.Insert(dbName, tableName, recordBatchJson[numBatches - 1]);
    EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  }

  // verify query result after rebuild
  auto rebuildStatus = rebuildStatusFuture.get();
  EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
  const auto limit = 100;
  auto postRebuildQueryStatus = database.Search(dbName, tableName, fieldName, queryFields, dimension, queryData, limit, queryResult, "ID < 10", true);
  EXPECT_TRUE(postRebuildQueryStatus.ok()) << postRebuildQueryStatus.message();
  EXPECT_LE(queryResult.GetSize(), 10) << "more than expected entries loaded";
  for (int i = 0; i < queryResult.GetSize(); i++) {
    EXPECT_LE(queryResult.GetArrayElement(i).GetInt("ID"), 10) << "invalid entry returned";
  }
}

TEST(DbServer, InvalidSparseVector) {
  std::string tempDir = std::filesystem::temp_directory_path() / std::filesystem::path("ut_db_server_invalid_sparse_vector");
  vectordb::engine::DBServer database;
  std::filesystem::remove_all(tempDir);
  const auto dbName = "MyDb";
  const auto tableName = "MyTable";
  size_t queryDimension = 4;
  database.LoadDB(dbName, tempDir, 150000, true);
  size_t tableId = 0;

  const std::string schema = R"_(
{
  "name": "MyTable",
  "fields": [
    {
      "name": "ID",
      "dataType": "INT",
      "primaryKey": true
    },
    {
      "name": "Doc",
      "dataType": "STRING"
    },
    {
      "name": "EmbeddingEuclidean",
      "dataType": "SPARSE_VECTOR_FLOAT",
      "dimensions": 4,
      "metricType": "EUCLIDEAN"
    }
  ]
}
    )_";

  const std::string records = R"_(
[
  {
    "ID": 1,
    "Doc": "len(indices) != len(values)",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.05, 0.61, 0.76]
    }
  },
  {
    "ID": 2,
    "Doc": "indices not increasing",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 3, 3],
        "values": [0.19, 0.81, 0.75, 0.11]
    }
  },
  {
    "ID": 3,
    "Doc": "index value exceeds dimension",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 4],
        "values": [0.36, 0.55, 0.47, 0.94]
    }
  },
  {
    "ID": 4,
    "Doc": "negative index",
    "EmbeddingEuclidean": {
        "indices": [-1, 2, 3],
        "values": [0.01, 0.85, 0.8]
    }
  },
  {
    "ID": 5,
    "Doc": "ok",
    "EmbeddingEuclidean": {
        "indices": [0, 1, 2, 3],
        "values": [0.24, 0.18, 0.22, 0.44]
    }
  }
]
    )_";

  auto createTableStatus = database.CreateTable(dbName, schema, tableId);
  EXPECT_TRUE(createTableStatus.ok()) << createTableStatus.message();
  vectordb::Json recordsJson;
  EXPECT_TRUE(recordsJson.LoadFromString(records));
  auto insertStatus = database.Insert(dbName, tableName, recordsJson);
  EXPECT_TRUE(insertStatus.ok()) << insertStatus.message();
  auto queryDataPtr = std::make_shared<vectordb::engine::SparseVector>(
      vectordb::engine::SparseVector({{0, 0.35}, {1, 0.55}, {2, 0.47}, {3, 0.94}}));

  auto rebuildOptions = {false, true};
  for (auto rebuild : rebuildOptions) {
    if (rebuild) {
      auto rebuildStatus = database.Rebuild();
      EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();
    }
    vectordb::Json result;
    const auto limit = 6;
    std::string expectedField = "EmbeddingEuclidean";
    auto queryFields = std::vector<std::string>{"ID", "Doc", expectedField};
    auto queryStatus = database.Search(dbName, tableName, expectedField, queryFields, queryDimension, queryDataPtr, limit, result, "", true);
    EXPECT_TRUE(queryStatus.ok()) << queryStatus.message();
    EXPECT_EQ(result.GetSize(), 1) << "invalid records should've been ignored";
    EXPECT_EQ(result.GetArrayElement(0).GetInt("ID"), 5);
  }
}
