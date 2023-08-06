#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <string>
#include <memory>
#include "db/db_server.hpp"
static PyObject *EpsillaError;

static PyObject *load_db(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject *use_db(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject *create_table(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject *insert(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject *query(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject *drop_table(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject *unload_db(PyObject *self, PyObject *args, PyObject *kwargs);

static std::string db_name;
static vectordb::engine::DBServer *db;

static PyMethodDef EpsillaMethods[] = {
    {"load_db", (PyCFunction)(void (*)(void))load_db, METH_VARARGS | METH_KEYWORDS, "Load the database"},
    {"unload_db", (PyCFunction)(void (*)(void))unload_db, METH_VARARGS | METH_KEYWORDS, "Unload the database"},
    {"use_db", (PyCFunction)(void (*)(void))use_db, METH_VARARGS | METH_KEYWORDS, "Use the database"},
    {"create_table", (PyCFunction)(void (*)(void))create_table, METH_VARARGS | METH_KEYWORDS, "create a table"},
    {"insert", (PyCFunction)(void (*)(void))insert, METH_VARARGS | METH_KEYWORDS, "insert record into the database"},
    {"query", (PyCFunction)(void (*)(void))query, METH_VARARGS | METH_KEYWORDS, "query the database"},
    {"drop_table", (PyCFunction)(void (*)(void))drop_table, METH_VARARGS | METH_KEYWORDS, "drop the table"},
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