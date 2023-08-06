#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <string>
#include <memory>
#include "db/db_server.hpp"
static PyObject *EpsillaError;

static PyObject *load_db(PyObject *self, PyObject *args);
static PyObject *use_db(PyObject *self, PyObject *args);
static PyObject *create_table(PyObject *self, PyObject *args);
static PyObject *insert(PyObject *self, PyObject *args);
static PyObject *query(PyObject *self, PyObject *args);

static std::string db_name;
static vectordb::engine::DBServer *db;

static PyMethodDef EpsillaMethods[] = {
    {"load_db", load_db, METH_VARARGS, "Load the database"},
    {"use_db", use_db, METH_VARARGS, "Use the database"},
    {"create_table", create_table, METH_VARARGS, "Use create the table"},
    {"insert", insert, METH_VARARGS, "insert record into the database"},
    {"query", query, METH_VARARGS, "query the database"},
    {NULL, NULL, 0, NULL} /* Sentinel */
};

static struct PyModuleDef epsilla = {
    PyModuleDef_HEAD_INIT,
    "epsilla",                                      /* name of module */
    "Epsilla - help you discover the vector space", /* module documentation, may be NULL */
    -1,                                             /* size of per-interpreter state of the module,
                                                       or -1 if the module keeps state in global variables. */
    EpsillaMethods};

PyMODINIT_FUNC
PyInit_epsilla(void);