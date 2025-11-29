#include <getopt.h>
#include <omp.h>
#include <unistd.h>

#include <cstring>
#include <ctime>
#include <cstdlib>
#include <iostream>

#include "server/server.hpp"
#include "logger/logger.hpp"
#include "db/config_manager.hpp"

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

void print_startup_config(uint16_t port, const std::string& port_source,
                          bool rebuild, bool is_leader,
                          const std::string& embedding_baseurl,
                          const std::string& config_filename) {
  std::cout << "\n";
  std::cout << "================================================================================\n";
  std::cout << "                        VectorDB Startup Configuration                          \n";
  std::cout << "================================================================================\n";

  std::cout << " Server Configuration:\n";
  std::cout << "   - Server Port              : " << port << " (" << port_source << ")\n";
  std::cout << "   - Rebuild Mode             : " << (rebuild ? "ENABLED" : "DISABLED") << "\n";
  std::cout << "   - Leader Mode              : " << (is_leader ? "ENABLED" : "DISABLED") << "\n";
  std::cout << "   - Embedding Service URL    : " << embedding_baseurl << "\n";
  if (!config_filename.empty()) {
    std::cout << "   - Config File              : " << config_filename << "\n";
  }
  std::cout << "\n";
  std::cout << "================================================================================\n\n";
}

int main(int argc, char *argv[]) {
  print_banner();

  // Initialize configuration manager with default values
  vectordb::engine::db::ConfigManager::Initialize();

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
  // Initialize port from environment variable SERVICE_PORT, default to 8888
  uint16_t port = 8888;
  std::string port_source = "default";
  const char* service_port_env = std::getenv("SERVICE_PORT");
  if (service_port_env != nullptr) {
    try {
      int env_port = std::stoi(service_port_env);
      if (env_port > 0 && env_port <= 65535) {
        port = static_cast<uint16_t>(env_port);
        port_source = "SERVICE_PORT environment variable";
        logger.Info("Port set to " + std::to_string(port) + " from SERVICE_PORT environment variable");
      } else {
        logger.Warning("Invalid SERVICE_PORT value: " + std::string(service_port_env) + ", using default: 8888");
      }
    } catch (const std::exception& e) {
      logger.Warning("Failed to parse SERVICE_PORT: " + std::string(service_port_env) + ", using default: 8888");
    }
  }
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
        try {
          int cmd_port = std::stoi(server_port);
          if (cmd_port > 0 && cmd_port <= 65535) {
            port = static_cast<uint16_t>(cmd_port);
            port_source = "command line argument";
            logger.Info("Port set to " + std::to_string(port) + " from command line argument");
          } else {
            logger.Error("Invalid port value: " + server_port + " (must be between 1 and 65535)");
            goto FAIL;
          }
        } catch (const std::exception& e) {
          logger.Error("Failed to parse port: " + server_port);
          goto FAIL;
        }
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

  // Print startup configuration
  print_startup_config(port, port_source, rebuild, is_leader, embedding_baseurl, config_filename);

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
