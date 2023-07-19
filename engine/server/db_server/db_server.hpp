#pragma once

#include <string>
#include "utils/status.hpp"
#include "db/db.hpp"

namespace vectordb {
namespace server {
namespace db {

class DBServer {
private:
  engine::DBPtr db_;

private:
  DBServer() = default;
  ~DBServer() = default;


public:
  static DBServer&
  GetInstance() {
    static DBServer db_server;
    return db_server;
  }

  Status Start();
  Status Stop();
};
}  // namespace db
}  // namespace server
}  // namespace vectordb
