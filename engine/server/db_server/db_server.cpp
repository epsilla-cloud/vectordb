//#include <omp.h>
#include <omp.h>
#include "server/db_server/db_server.hpp"

namespace vectordb {
namespace server {
namespace db {


Status DBServer::Start() {
  return Status::OK();
}

Status DBServer::Stop() {
  return Status::OK();
}

}
}
}

