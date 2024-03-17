#include <getopt.h>
#include <omp.h>
#include <unistd.h>

#include <cstring>
#include <ctime>
#include <iostream>

#include "server/server.hpp"
#include "logger/logger.hpp"

#include "db/execution/aggregation.hpp"

void print_help(const std::string &app_name) {
  std::cout << std::endl
            << "Usage: " << app_name << " [OPTIONS]" << std::endl
            << std::endl;
  std::cout << "  Options:" << std::endl;
  std::cout << "   -h --help                 Print this help." << std::endl;
  std::cout << "   -p --port port_number     Specify the server port." << std::endl;
  // std::cout << "   -c --conf_file filename   Read configuration from the file." << std::endl;
  std::cout << std::endl;
}

void print_banner() {
  std::cout << std::endl;
  std::cout << "Welcome to use Epsilla Vector Database!" << std::endl;
  std::cout << std::endl;
}

int main(int argc, char *argv[]) {

    vectordb::engine::execution::MinAggregator<std::string> minAggregator;
    minAggregator.addValue("group1", 100);
    minAggregator.addValue("group1", 50.5);
    minAggregator.addValue("group2", 200);

    std::cout << "Min Aggregation Results:" << std::endl;
    for (auto it = minAggregator.begin(); it != minAggregator.end(); ++it) {
        std::cout << "Key: " << it->first << ", Value: " << it->second << std::endl;
    }

    // Example with a range-based for loop
    vectordb::engine::execution::MaxAggregator<std::string> maxAggregator;
    maxAggregator.addValue("group1", 150);
    maxAggregator.addValue("group1", 200.5);
    maxAggregator.addValue("group2", 100);

    std::cout << "Max Aggregation Results:" << std::endl;
    for (const auto& pair : maxAggregator) {
        std::cout << "Key: " << pair.first << ", Value: " << pair.second << std::endl;
    }

    vectordb::engine::execution::MinAggregator<double> minAggregator2;
    minAggregator2.addValue(1.5, 100);
    minAggregator2.addValue(1.5, 50.5);
    minAggregator2.addValue(2.3, 200);

    std::cout << "Min Aggregation Results:" << std::endl;
    for (auto it = minAggregator2.begin(); it != minAggregator2.end(); ++it) {
        std::cout << "Key: " << it->first << ", Value: " << it->second << std::endl;
    }

  return 0;

  print_banner();
  vectordb::engine::Logger logger;

  static struct option long_options[] = {{"conf_file", required_argument, nullptr, 'c'},
                                         {"help", no_argument, nullptr, 'h'},
                                         {"port", required_argument, 0, 'p'},
                                         {"rebuild", required_argument, 0, 'r'},
                                         {"leader", required_argument, 0, 'l'},
                                         {"embedding_baseurl", required_argument, 0, 'e'},
                                         {nullptr, 0, nullptr, 0}};

  int option_index = 0;

  std::string config_filename;
  std::string app_name = argv[0];

  vectordb::Status status;
  vectordb::server::Server &server = vectordb::server::Server::GetInstance();

  // if (argc < 2) {
  //   print_help(app_name);
  //   goto FAIL;
  // }

  int value;
  uint16_t port = 8888;
  bool rebuild = true;
  bool is_leader = true;
  std::string embedding_baseurl = "http://localhost:8889";
  while ((value = getopt_long(argc, argv, "c:p:r:l:e:h", long_options, &option_index)) != -1) {
    switch (value) {
      case 'c': {
        char *config_filename_ptr = strdup(optarg);
        config_filename = config_filename_ptr;
        free(config_filename_ptr);
        std::cout << "Loading configuration from: " << config_filename << std::endl;
        break;
      }
      case 'p': {
        std::string server_port = optarg;
        port = (uint16_t)(stoi(server_port));
        break;
      }
      case 'r': {
        std::string start_rebuild = optarg;
        rebuild = start_rebuild[0] == 't' || start_rebuild[0] == 'T';
        break;
      }
      case 'l': {
        std::string leader = optarg;
        is_leader = leader[0] == 't' || leader[0] == 'T';
        break;
      }
      case 'e': {
        embedding_baseurl = optarg;
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

  status = server.Start(port, rebuild, is_leader, embedding_baseurl);
  if (status.ok()) {
    logger.Info("Epsilla Vector Database server started successfully!");
    logger.Info(std::string("Server running on http://0.0.0.0:") + std::to_string(port));
  } else {
    logger.Error(status.message());
    goto FAIL;
  }

  /* wait signal */
  pause();

  return EXIT_SUCCESS;

FAIL:
  logger.Error("Epsilla Vector Database server exit...");
  return EXIT_FAILURE;
}
