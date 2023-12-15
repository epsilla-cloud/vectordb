#define PY_SSIZE_T_CLEAN
#include "bindings/python/interface.h"

#include <Python.h>

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "db/db_server.hpp"
#include "server/web_server/web_controller.hpp"

PyMODINIT_FUNC
PyInit_epsilla(void) {
  PyObject *m;

  m = PyModule_Create(&epsilla);
  if (m == NULL)
    return NULL;

  EpsillaError = PyErr_NewException("epsilla.error", NULL, NULL);
  Py_XINCREF(EpsillaError);
  if (PyModule_AddObject(m, "error", EpsillaError) < 0) {
    Py_XDECREF(EpsillaError);
    Py_CLEAR(EpsillaError);
    Py_XDECREF(m);
    return NULL;
  }

  db = new vectordb::engine::DBServer();

  return m;
}

static PyObject *load_db(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {"db_name", "db_path", NULL};
  const char *namePtr, *pathPtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss", (char **)keywords, &namePtr, &pathPtr))
    return NULL;

  auto name = std::string(namePtr), path = std::string(pathPtr);
  if (name.empty()) {
    PyErr_SetString(PyExc_Exception, "empty db name");
    return NULL;
  }

  if (path.empty()) {
    PyErr_SetString(PyExc_Exception, "empty path name");
    return NULL;
  }

  std::unordered_map<std::string, std::string> headers;
  auto status = db->LoadDB(
      name,
      path,
      vectordb::server::web::InitTableScale,
      // TODO: make it variable
      true,
      headers);
  return PyLong_FromLong(status.code());
}

static PyObject *use_db(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {"db_name", NULL};
  const char *namePtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", (char **)keywords, &namePtr))
    return NULL;

  db_name = namePtr;
  return PyLong_FromLong(0);
}

static PyObject *create_table(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {"table_name", "table_fields", NULL};
  const char *tableNamePtr;
  PyObject *tableFieldsListPtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO", (char **)keywords, &tableNamePtr, &tableFieldsListPtr))
    return NULL;

  PyObject *fieldKeyValue = PyUnicode_DecodeUTF8("fields", strlen("fields"), "ignore");

  PyObject *schemaObj = PyDict_New();
  PyDict_SetItemString(schemaObj, "name", PyUnicode_DecodeUTF8(tableNamePtr, strlen(tableNamePtr), "ignore"));
  PyDict_SetItem(schemaObj, fieldKeyValue, tableFieldsListPtr);

  PyObject *json_module = PyImport_ImportModule("json");

  if (json_module == NULL) {
    PyErr_SetString(PyExc_Exception, "Unable to import json module");
    return NULL;
  }

  PyObject *dumps_func = PyObject_GetAttrString(json_module, "dumps");
  Py_DECREF(json_module);

  if (dumps_func == NULL || !PyCallable_Check(dumps_func)) {
    PyErr_SetString(PyExc_Exception, "Unable to get address of json.dumps method");
    Py_XDECREF(dumps_func);
    return NULL;
  }

  PyObject *args_tuple = PyTuple_Pack(1, schemaObj);
  PyObject *kwargs_dict = PyDict_New();  // You can pass keyword arguments here if needed

  PyObject *json_str = PyObject_Call(dumps_func, args_tuple, kwargs_dict);

  Py_DECREF(dumps_func);
  Py_DECREF(args_tuple);
  Py_DECREF(kwargs_dict);

  if (json_str == NULL) {
    PyErr_SetString(PyExc_Exception, "unable to dump records as JSON");
    return NULL;
  }

  // Convert the PyObject to a C-style string (UTF-8)
  const char *utf8_str = PyUnicode_AsUTF8(json_str);

  if (utf8_str == NULL) {
    return NULL;
  }

  std::string schema_json(utf8_str);

  Py_DECREF(tableFieldsListPtr);

  // TODO: add auto embedding here
  size_t table_id;
  auto status = db->CreateTable(db_name, schema_json, table_id, nullptr);
  if (!status.ok()) {
    PyErr_SetString(PyExc_Exception, status.message().c_str());
    return NULL;
  }
  return PyLong_FromLong(int(status.code()));
}

static PyObject *insert(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {"table_name", "records", NULL};
  const char *tableNamePtr;
  PyObject *recordListPtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO", (char **)keywords, &tableNamePtr, &recordListPtr))
    return NULL;

  std::string tableName = tableNamePtr;

  PyObject *json_module = PyImport_ImportModule("json");

  if (json_module == NULL) {
    PyErr_SetString(PyExc_Exception, "Unable to import json module");
    return NULL;
  }

  PyObject *dumps_func = PyObject_GetAttrString(json_module, "dumps");
  Py_DECREF(json_module);

  if (dumps_func == NULL || !PyCallable_Check(dumps_func)) {
    PyErr_SetString(PyExc_Exception, "Unable to get address of json.dumps method");
    Py_XDECREF(dumps_func);
    return NULL;
  }

  PyObject *args_tuple = PyTuple_Pack(1, recordListPtr);
  PyObject *kwargs_dict = PyDict_New();  // You can pass keyword arguments here if needed

  PyObject *json_str = PyObject_Call(dumps_func, args_tuple, kwargs_dict);

  Py_DECREF(dumps_func);
  Py_DECREF(args_tuple);
  Py_DECREF(kwargs_dict);

  if (json_str == NULL) {
    PyErr_SetString(PyExc_Exception, "unable to dump records as JSON");
    return NULL;
  }

  // Convert the PyObject to a C-style string (UTF-8)
  const char *utf8_str = PyUnicode_AsUTF8(json_str);

  if (utf8_str == NULL) {
    return NULL;
  }

  auto records = vectordb::Json();
  records.LoadFromString(std::string(utf8_str));
  Py_DECREF(json_str);

  std::unordered_map<std::string, std::string> headers;

  auto status = db->Insert(db_name, tableName, records, headers);
  return PyLong_FromLong(int(status.code()));
}

static PyObject *delete_by_pk(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {"table_name", "primary_keys", NULL};
  const char *tableNamePtr;
  PyObject *pkListPtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO", (char **)keywords, &tableNamePtr, &pkListPtr))
    return NULL;
  Py_XINCREF(pkListPtr);

  std::string tableName = tableNamePtr;

  PyObject *json_module = PyImport_ImportModule("json");

  if (json_module == NULL) {
    PyErr_SetString(PyExc_Exception, "Unable to import json module");
    return NULL;
  }

  PyObject *dumps_func = PyObject_GetAttrString(json_module, "dumps");
  Py_XDECREF(json_module);

  if (dumps_func == NULL || !PyCallable_Check(dumps_func)) {
    PyErr_SetString(PyExc_Exception, "Unable to get address of json.dumps method");
    Py_XDECREF(dumps_func);
    return NULL;
  }

  PyObject *args_tuple = PyTuple_Pack(1, pkListPtr);
  PyObject *kwargs_dict = PyDict_New();  // You can pass keyword arguments here if needed
  Py_XINCREF(args_tuple);
  Py_XINCREF(kwargs_dict);

  PyObject *json_str = PyObject_Call(dumps_func, args_tuple, kwargs_dict);
  Py_XINCREF(dumps_func);

  Py_XDECREF(dumps_func);
  Py_XDECREF(args_tuple);
  Py_XDECREF(kwargs_dict);
  Py_XDECREF(pkListPtr);

  if (json_str == NULL) {
    PyErr_SetString(PyExc_Exception, "unable to dump records as JSON");
    return NULL;
  }

  // Convert the PyObject to a C-style string (UTF-8)
  const char *utf8_str = PyUnicode_AsUTF8(json_str);

  if (utf8_str == NULL) {
    return NULL;
  }

  auto records = vectordb::Json();
  records.LoadFromString(std::string(utf8_str));
  Py_XDECREF(json_str);

  // TODO: suppport delete by filter.
  auto status = db->Delete(db_name, tableName, records, "");
  std::cerr << status.message() << std::endl;
  return PyLong_FromLong(int(status.code()));
}

static PyObject *query(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {
      "table_name",
      "query_field",
      "query_vector",
      "response_fields",
      "limit",
      "filter",
      "with_distance",
      NULL};
  const char *tableNamePtr, *queryFieldPtr, *queryFilterPtr;
  int limit, withDistance;
  PyObject *queryVector, *responseFields;

  if (!PyArg_ParseTupleAndKeywords(
          args,
          kwargs,
          "ssOOisp",
          (char **)keywords,
          &tableNamePtr,
          &queryFieldPtr,
          &queryVector,
          &responseFields,
          &limit,
          &queryFilterPtr,
          &withDistance)) {
    return NULL;
  }
  Py_XINCREF(queryVector);
  Py_XINCREF(responseFields);
  auto queryFields = std::vector<std::string>();

  Py_ssize_t queryVectorSize = PyList_Size(queryVector);
  std::string tableName = tableNamePtr, queryField = queryFieldPtr, queryFilter = queryFilterPtr;
  auto vectorArr = std::make_unique<float[]>(queryVectorSize);
  for (Py_ssize_t i = 0; i < queryVectorSize; ++i) {
    PyObject *elem = PyList_GetItem(queryVector, i);
    Py_XINCREF(elem);
    vectorArr[i] = PyFloat_AsDouble(elem);
    Py_XDECREF(elem);
  }
  Py_ssize_t responseFieldsSize = PyList_Size(responseFields);
  for (Py_ssize_t i = 0; i < responseFieldsSize; ++i) {
    PyObject *elem = PyObject_Str(PyList_GetItem(responseFields, i));
    Py_XINCREF(elem);
    std::string field = PyUnicode_AsUTF8(elem);
    Py_XDECREF(elem);
    queryFields.push_back(field);
  }
  Py_XDECREF(queryVector);
  auto result = vectordb::Json();
  std::unordered_map<std::string, std::string> headers;
  auto status = db->Search(
      db_name,
      tableName,
      queryField,
      queryFields,
      queryVectorSize,
      vectorArr.get(),
      limit,
      result,
      queryFilter,
      withDistance,
      headers);

  if (!status.ok()) {
    PyErr_SetString(PyExc_Exception, status.message().c_str());
    return NULL;
  }

  // Import the json module
  PyObject *json_module = PyImport_ImportModule("json");
  if (json_module == NULL) {
    PyErr_SetString(PyExc_Exception, "Unable to import json module");
    return NULL;
  }

  // Get a reference to the json.loads function
  PyObject *loads_func = PyObject_GetAttrString(json_module, "loads");
  if (loads_func == NULL || !PyCallable_Check(loads_func)) {
    PyErr_SetString(PyExc_Exception, "Unable to get address of json.loads method");
    Py_XDECREF(loads_func);
    return NULL;
  }
  PyObject *resultString = PyUnicode_FromString(result.DumpToString().c_str());
  PyObject *args_tuple = PyTuple_Pack(1, resultString);
  PyObject *kwargs_dict = PyDict_New();  // You can pass keyword arguments here if needed

  PyObject *response = PyObject_Call(loads_func, args_tuple, kwargs_dict);

  Py_XDECREF(loads_func);
  Py_XDECREF(args_tuple);
  Py_XDECREF(kwargs_dict);

  if (response == NULL) {
    PyErr_SetString(PyExc_Exception, "unable to json.loads response from string");
    return NULL;
  }

  PyObject *ret = PyTuple_Pack(2, PyLong_FromLong(long(status.code())), response);
  return ret;
}

static PyObject *drop_table(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {"table_name", NULL};
  const char *tableNamePtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", (char **)keywords, &tableNamePtr))
    return NULL;

  auto tableName = std::string(tableNamePtr);
  if (tableName.empty()) {
    PyErr_SetString(PyExc_Exception, "empty table name");
    return NULL;
  }

  auto status = db->DropTable(db_name, tableName);
  return PyLong_FromLong(status.code());
}

static PyObject *unload_db(PyObject *self, PyObject *args, PyObject *kwargs) {
  static const char *keywords[] = {"db_name", NULL};
  const char *dbNamePtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", (char **)keywords, &dbNamePtr))
    return NULL;

  auto dbName = std::string(dbNamePtr);
  if (dbName.empty()) {
    PyErr_SetString(PyExc_Exception, "empty db name");
    return NULL;
  }

  auto status = db->DropTable(db_name, dbName);
  return PyLong_FromLong(status.code());
}
