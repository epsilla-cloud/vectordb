#include <getopt.h>
#include <omp.h>
#include <unistd.h>

#include <cstring>
#include <ctime>
#include <iostream>

#include "db/ann_graph_segment.hpp"
#include "db/catalog/basic_meta_impl.hpp"
#include "db/catalog/meta.hpp"
#include "db/db.hpp"
#include "db/execution/vec_search_executor.hpp"
#include "db/index/knn/knn.hpp"
#include "db/index/nsg/nsg.hpp"
#include "db/index/space_cosine.hpp"
#include "db/index/space_ip.hpp"
#include "db/index/space_l2.hpp"
#include "db/table.hpp"
#include "db/table_segment_mvp.hpp"
#include "server/server.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/concurrent_queue.hpp"
#include "utils/json.hpp"
#include "utils/status.hpp"
#include "db/table_mvp.hpp"
#include "db/db_mvp.hpp"


void print_help(const std::string &app_name) {
  std::cout << std::endl
            << "Usage: " << app_name << " [OPTIONS]" << std::endl
            << std::endl;
  std::cout << "  Options:" << std::endl;
  std::cout << "   -h --help                 Print this help." << std::endl;
  std::cout << "   -c --conf_file filename   Read configuration from the file." << std::endl;
  std::cout << std::endl;
}

void print_banner() {
  std::cout << std::endl;
  std::cout << "Welcome to use Epsilla Vector Database!" << std::endl;
  std::cout << std::endl;
}

int main(int argc, char *argv[]) {
  print_banner();

  
  // std::srand(std::time(nullptr));
  // int x = 10, y = 10;
  // int n = x * y;
  // int dim = 1536;
  // float *data = new float[n * dim];
  // for (int i = 0; i < n; i++) {
  //   for (int p = 0; p < dim; ++p) {
  //     data[i * dim + p] = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
  //   }
  // }
  // std::cout << "here" << std::endl;
  // vectordb::engine::index::Graph knng(n);
  // vectordb::engine::index::KNNGraph graph(n, dim, 100, data, knng);

  // auto ann_graph_segment = std::make_shared<vectordb::engine::ANNGraphSegment>("/tmp/epsilla-03", 1, 3);
  // // ann_graph_segment->BuildFromVectorTable(data, n, dim);
  // ann_graph_segment->Debug();
  // // ann_graph_segment->SaveANNGraph("/tmp/epsilla-03", 1, 3);

  vectordb::engine::meta::MetaPtr meta = std::make_shared<vectordb::engine::meta::BasicMetaImpl>();
  std::string db_name = "test_db";
  std::string db_catalog_path = "/tmp/epsilla-01/";
  meta->LoadDatabase(db_catalog_path, db_name);

  vectordb::engine::meta::DatabaseSchema db_schema;
  meta->GetDatabase(db_name, db_schema);
  auto db = std::make_shared<vectordb::engine::DBMVP>(db_schema);

  vectordb::engine::meta::TableSchema table_schema;
  meta->GetTable(db_name, "test-table-7", table_schema);

  auto table_mvp = db->GetTable("test-table-7");
  
  table_mvp->Rebuild(db_catalog_path);
  std::cout << table_mvp->table_segment_->record_number_ << std::endl;
  std::cout << table_mvp->ann_graph_segment_[0]->record_number_ << std::endl;
  std::cout << table_mvp->ann_graph_segment_[1]->record_number_ << std::endl;
  

  std::string json_string = R"([
    {
      "id": 1,
      "doc": "This is a test document 1.",
      "vec1": [1.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document.",
      "testBool": false
    },
    {
      "id": 2,
      "doc": "This is a test document 2.",
      "vec1": [2.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document.",
      "testBool": false
    },
    {
      "id": 3,
      "doc": "This is a test document 3.",
      "vec1": [3.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document.45656",
      "testBool": false
    },
    {
      "id": 4,
      "doc": "This is a test document 4.",
      "vec1": [4.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document.",
      "testBool": false
    },
    {
      "id": 5,
      "doc": "This is a test document 5.",
      "vec1": [5.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document.",
      "testBool": false
    },
    {
      "id": 6,
      "doc": "This is a test document 6.",
      "vec1": [6.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document.",
      "testBool": true
    },
    {
      "id": 7,
      "doc": "This is a test document 7.",
      "vec1": [7.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document435.",
      "testBool": true
    },
    {
      "id": 8,
      "doc": "This is a test document 8.",
      "vec1": [8.0, 2.0, 3.0, 4.0],
      "vec2": [0.1, 0.4, 0.2, 0.3, 0.5, 0.1, 0.7, 0.6],
      "doc2": "This is another document.",
      "testBool": false
    }

    ])";

  vectordb::Json json;
  bool load_success = json.LoadFromString(json_string);

  for (auto w = 0; w < 1; ++w) {
    auto status2 = table_mvp->Insert(table_schema, json);
    if (status2.ok()) {
      std::cout << "Insert successfully!" << std::endl;
    } else {
      std::cout << status2.message() << std::endl;
    }
  }
  std::cout << table_mvp->table_segment_->record_number_ << std::endl;
  std::cout << table_mvp->ann_graph_segment_[0]->record_number_ << std::endl;
  std::cout << table_mvp->ann_graph_segment_[1]->record_number_ << std::endl;

  vectordb::Json result;
  float query[4] {5.0, 2.0, 3.0, 4.0};
  std::vector<std::string> fields {"id", "vec2", "doc2"};
  table_mvp->Search("vec1", fields, query, 5, result);
  std::cout << result.DumpToString() << std::endl;

  // table_mvp->Rebuild(db_catalog_path);
  // std::cout << table_mvp->table_segment_->record_number_ << std::endl;
  // std::cout << table_mvp->ann_graph_segment_[0]->record_number_ << std::endl;
  // std::cout << table_mvp->ann_graph_segment_[1]->record_number_ << std::endl;
  

  // table.Debug(table_schema);
  // auto status3 = table.SaveTableSegment(table_schema, db_catalog_path);
  // // if (status3.ok()) {
  // //   std::cout << "Save successfully!" << std::endl;
  // // } else {
  // //   std::cout << status3.message() << std::endl;
  // // }

  // std::srand(std::time(nullptr));
  // int x = 100, y = 100;
  // int n = x * y;
  // int dim = 1536;
  // float *data = new float[n * dim];
  // for (int i = 0; i < n; i++) {
  //   for (int p = 0; p < dim; ++p) {
  //     data[i * dim + p] = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
  //   }
  // }
  // std::cout << "here" << std::endl;
  // // vectordb::engine::index::Graph knng(n);
  // // vectordb::engine::index::KNNGraph graph(n, dim, 100, data, knng);

  // auto ann_graph_segment = std::make_shared<vectordb::engine::ANNGraphSegment>();
  // int64_t graph_n = 6000;
  // ann_graph_segment->BuildFromVectorTable(data, graph_n, dim);
  // // ann_graph_segment->Debug();
  // std::cout << ann_graph_segment->navigation_point_ << std::endl;

  // auto space_ = new vectordb::L2Space(dim);
  // auto fstdistfunc_ = space_->get_dist_func();
  // auto dist_func_param_ = space_->get_dist_func_param();

  // omp_set_max_active_levels(2);
  // omp_set_num_threads(4);
  // std::cout << "start query" << std::endl;
  // float *query = new float[dim];

  // // #pragma omp parallel
  // double total_hit = 0;
  // int total_run = 100;
  // int K = 50;
  // for (int w = 0; w < total_run; ++w)
  // {
  //   for (int p = 0; p < dim; ++p) {
  //     query[p] = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
  //     // std::cout << query[p] << " ";
  //   }
  //   auto executor = std::make_shared<vectordb::engine::execution::VecSearchExecutor>(
  //       graph_n, dim,
  //       ann_graph_segment->navigation_point_,
  //       ann_graph_segment->offset_table_,
  //       ann_graph_segment->neighbor_list_,
  //       data,
  //       fstdistfunc_, dist_func_param_,
  //       4, 500, 500, 15);
  //   executor->brute_force_queue_.resize(10000000);
    
  //   // std::cout << std::endl;
  //   // std::cout << "Query" << std::endl;
  //   // for (int j = 0; j < dim; ++j) {
  //   //   std::cout << query[j] << " ";
  //   // }
  //   // std::cout << std::endl;
  //   // std::cout << "Compare" << std::endl;
  //   std::vector<int64_t> result1, result2;
  //   // executor->brute_force_search_ = false;
  //   int64_t k_result1, k_result2;
  //   std::cout << "start search 1" << std::endl;
  //   executor->Search(query, K, n, k_result1);
  //   for (int j = 0; j < k_result1; ++j) {
  //     // std::cout << executor->search_result_[j] << " ";
  //     result1.push_back(executor->search_result_[j]);
  //   }
  //   std::cout << "start search 2" << std::endl;
  //   // std::cout << std::endl;
  //   executor->brute_force_search_ = true;
  //   executor->Search(query, K, n, k_result2);
  //   for (int j = 0; j < k_result2; ++j) {
  //     // std::cout << executor->search_result_[j] << " ";
  //     result2.push_back(executor->search_result_[j]);
  //   }
  //   std::cout << "start search 3" << std::endl;
  //   int count = 0;
  //   for (int i = 0; i < k_result1; ++i) {
  //     for (int j = 0; j < k_result2; ++j) {
  //       if (result1[i] == result2[j]) {
  //         count++;
  //       }
  //     }
  //   }
  //   total_hit += count;
  //   std::cout << count << std::endl;
  // }
  // std::cout << total_hit / (total_run * K) << std::endl;


  static struct option long_options[] = {{"conf_file", required_argument, nullptr, 'c'},
                                         {"help", no_argument, nullptr, 'h'},
                                         {nullptr, 0, nullptr, 0}};

  int option_index = 0;

  std::string config_filename;
  std::string app_name = argv[0];

  vectordb::Status status;
  vectordb::server::Server &server = vectordb::server::Server::GetInstance();

  if (argc < 2) {
    print_help(app_name);
    goto FAIL;
  }

  int value;
  while ((value = getopt_long(argc, argv, "c:dh", long_options, &option_index)) != -1) {
    switch (value) {
      case 'c': {
        char *config_filename_ptr = strdup(optarg);
        config_filename = config_filename_ptr;
        free(config_filename_ptr);
        std::cout << "Loading configuration from: " << config_filename << std::endl;
        break;
      }
      case 'h':
        print_help(app_name);
        return EXIT_SUCCESS;
      default:
        print_help(app_name);
        break;
    }
  }

  server.Init(config_filename);

  status = server.Start();
  if (status.ok()) {
    std::cout << "Epsilla Vector Database server started successfully!" << std::endl;
  } else {
    std::cout << status.message() << std::endl;
    goto FAIL;
  }

  /* wait signal */
  pause();

  return EXIT_SUCCESS;

FAIL:
  std::cout << "Epsilla Vector Database server exit..." << std::endl;
  return EXIT_FAILURE;
}
