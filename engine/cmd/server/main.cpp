#include <getopt.h>
#include <omp.h>
#include <unistd.h>

#include <cstring>
#include <ctime>
#include <iostream>

#include "server/server.hpp"

void print_help(const std::string &app_name)
{
  std::cout << std::endl
            << "Usage: " << app_name << " [OPTIONS]" << std::endl
            << std::endl;
  std::cout << "  Options:" << std::endl;
  std::cout << "   -h --help                 Print this help." << std::endl;
  std::cout << "   -p --port port_number     Specify the server port." << std::endl;
  // std::cout << "   -c --conf_file filename   Read configuration from the file." << std::endl;
  std::cout << std::endl;
}

void print_banner()
{
  std::cout << std::endl;
  std::cout << "Welcome to use Epsilla Vector Database!" << std::endl;
  std::cout << std::endl;
}

int main(int argc, char *argv[])
{
  print_banner();

  static struct option long_options[] = {{"conf_file", required_argument, nullptr, 'c'},
                                         {"help", no_argument, nullptr, 'h'},
                                         {"port", required_argument, 0, 'p'},
                                         {"rebuild", required_argument, 0, 'r'},
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
  while ((value = getopt_long(argc, argv, "c:p:r:h", long_options, &option_index)) != -1)
  {
    switch (value)
    {
    case 'c':
    {
      char *config_filename_ptr = strdup(optarg);
      config_filename = config_filename_ptr;
      free(config_filename_ptr);
      std::cout << "Loading configuration from: " << config_filename << std::endl;
      break;
    }
    case 'p':
    {
      std::string server_port = optarg;
      port = (uint16_t)(stoi(server_port));
      break;
    }
    case 'r':
    {
      std::string start_rebuild = optarg;
      rebuild = start_rebuild[0] == 't' || start_rebuild[0] == 'T';
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

  status = server.Start(port, rebuild);
  if (status.ok())
  {
    std::cout << "Epsilla Vector Database server started successfully!" << std::endl;
    std::cout << "Server running on http://localhost:" << port << std::endl;
  }
  else
  {
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
