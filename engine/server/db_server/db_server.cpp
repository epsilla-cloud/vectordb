//#include <omp.h>
#include </opt/homebrew/Cellar/libomp/16.0.6/include/omp.h>
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

