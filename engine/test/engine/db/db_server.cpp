
#include "db/db_server.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
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

  struct TestCase {
    std::string searchFieldName;
    std::vector<std::string> expectedOrder;
  };
  std::vector<TestCase> testcases = {
      {"EmbeddingEuclidean", {"Moscow", "Berlin", "Shanghai", "San Francisco", "London"}},
      {"EmbeddingDotProduct", {"Moscow", "Berlin", "San Francisco", "London", "Shanghai"}},
      {"EmbeddingCosine", {"Moscow", "Shanghai", "Berlin", "San Francisco", "London"}}};
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
  }

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

  // check rebuild
  auto rebuildStatus = database.Rebuild();
  EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();

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

TEST(DbServer, RebuildSparseVector) {
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

  // check rebuild
  auto rebuildStatus = database.Rebuild();
  EXPECT_TRUE(rebuildStatus.ok()) << rebuildStatus.message();

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
  }

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
