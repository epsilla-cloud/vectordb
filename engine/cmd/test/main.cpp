#include <getopt.h>
#include <omp.h>
#include <unistd.h>

#include <string>
#include <ctime>
#include <iostream>

#include "db/db_server.hpp"

int main(int argc, char *argv[])
{
    auto db = new vectordb::engine::DBServer();

    auto path = std::string("/tmp/db2");
    db->LoadDB(
        "whatever",
        path,
        150000,
        // TODO: make it variable
        true);
    db->CreateTable("whatever", );
    return 0;
}
