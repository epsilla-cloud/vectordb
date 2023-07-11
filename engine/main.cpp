#include <getopt.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

#include "server/server.hpp"
#include "utils/status.hpp"
#include "catalog/meta.hpp"


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
  std::cout << "Epsilla server exit..." << std::endl;
  return EXIT_FAILURE;
}
