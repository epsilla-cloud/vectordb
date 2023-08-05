#define PY_SSIZE_T_CLEAN
#include <string>
#include <Python.h>
#include "bindings/python/interface.h"
#include "db/db_server.hpp"
#include "server/web_server/web_controller.hpp"

static PyObject *
spam_system(PyObject *self, PyObject *args)
{
  const char *command;
  int sts;

  if (!PyArg_ParseTuple(args, "s", &command))
    return NULL;
  sts = system(command);
  if (sts < 0)
  {
    PyErr_SetString(EpsillaError, "System command failed");
    return NULL;
  }
  return PyLong_FromLong(sts);
}

PyMODINIT_FUNC
PyInit_epsilla(void)
{
  PyObject *m;

  m = PyModule_Create(&epsilla);
  if (m == NULL)
    return NULL;

  EpsillaError = PyErr_NewException("epsilla.error", NULL, NULL);
  Py_XINCREF(EpsillaError);
  if (PyModule_AddObject(m, "error", EpsillaError) < 0)
  {
    Py_XDECREF(EpsillaError);
    Py_CLEAR(EpsillaError);
    Py_DECREF(m);
    return NULL;
  }

  db = new vectordb::engine::DBServer();

  return m;
}

static PyObject *load_db(PyObject *self, PyObject *args)
{
  const char *namePtr, *pathPtr;
  int sts;

  if (!PyArg_ParseTuple(args, "ss", &namePtr, &pathPtr))
    return NULL;
  auto name = std::string(namePtr), path = std::string(pathPtr);
  auto status = db->LoadDB(name, path, vectordb::server::web::InitTableScale);
  return PyLong_FromLong(status.code());
}

static PyObject *use_db(PyObject *self, PyObject *args)
{
  const char *namePtr;

  if (!PyArg_ParseTuple(args, "s", &namePtr))
    return NULL;

  db_name = namePtr;
  return PyLong_FromLong(0);
}
static PyObject *create_table(PyObject *self, PyObject *args) {}
static PyObject *insert(PyObject *self, PyObject *args) {}
static PyObject *query(PyObject *self, PyObject *args) {}
