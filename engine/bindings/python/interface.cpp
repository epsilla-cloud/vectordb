#define PY_SSIZE_T_CLEAN
#include <string>
#include <vector>
#include <Python.h>
#include <memory>
#include "bindings/python/interface.h"
#include "db/db_server.hpp"
#include "server/web_server/web_controller.hpp"

const std::string tableSchemaKey_name = "name",
                  tableSchemaKey_dataType = "dataType",
                  tableSchemaKey_dimensions = "dimensions",
                  tableSchemaKey_autoEmbedding = "autoEmbedding";

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
    Py_XDECREF(m);
    return NULL;
  }

  db = new vectordb::engine::DBServer();

  return m;
}

static PyObject *load_db(PyObject *self, PyObject *args, PyObject *kwargs)
{
  static const char *keywords[] = {"db_name", "db_path", NULL};
  const char *namePtr, *pathPtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss", (char **)keywords, &namePtr, &pathPtr))
    return NULL;

  auto name = std::string(namePtr), path = std::string(pathPtr);
  if (name.empty())
  {
    PyErr_SetString(PyExc_Exception, "empty db name");
    return NULL;
  }

  if (path.empty())
  {
    PyErr_SetString(PyExc_Exception, "empty path name");
    return NULL;
  }

  auto status = db->LoadDB(
      name,
      path,
      vectordb::server::web::InitTableScale,
      // TODO: make it variable
      true);
  return PyLong_FromLong(status.code());
}

static PyObject *use_db(PyObject *self, PyObject *args, PyObject *kwargs)
{
  static const char *keywords[] = {"db_name", NULL};
  const char *namePtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", (char **)keywords, &namePtr))
    return NULL;

  db_name = namePtr;
  return PyLong_FromLong(0);
}

static PyObject *create_table(PyObject *self, PyObject *args, PyObject *kwargs)
{

  static const char *keywords[] = {"table_name", "table_fields", NULL};
  const char *tableNamePtr;
  PyObject *tableFieldsListPtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO", (char **)keywords, &tableNamePtr, &tableFieldsListPtr))
    return NULL;

  vectordb::engine::meta::TableSchema schema;
  schema.name_ = tableNamePtr;

  // Iterate through the list and extract dictionaries
  Py_ssize_t list_size = PyList_Size(tableFieldsListPtr);

  for (Py_ssize_t i = 0; i < list_size; ++i)
  {
    PyObject *dict_obj = PyList_GetItem(tableFieldsListPtr, i);
    vectordb::engine::meta::FieldSchema field;
    field.id_ = i;

    if (!PyDict_Check(dict_obj))
    {
      PyErr_SetString(PyExc_TypeError, "List must contain dictionaries");
      return NULL;
    }

    PyObject *nameKey = PyUnicode_DecodeUTF8(tableSchemaKey_name.c_str(), tableSchemaKey_name.size(), "strict");
    PyObject *nameValue = PyObject_Str(PyDict_GetItem(dict_obj, nameKey));
    const char *fieldNamePtr = PyUnicode_AsUTF8(nameValue);
    if (fieldNamePtr != NULL)
    {
      field.name_ = fieldNamePtr;
    }
    else
    {
      PyErr_SetString(PyExc_TypeError, "invalid content: ID is not valid UTF8 string");
      return NULL;
    }

    Py_DECREF(nameKey);
    Py_DECREF(nameValue);

    PyObject *dataTypeKey = PyUnicode_DecodeUTF8(tableSchemaKey_dataType.c_str(), tableSchemaKey_dataType.size(), "strict");
    PyObject *dataTypeValue = PyObject_Str(PyDict_GetItem(dict_obj, dataTypeKey));
    const char *dataTypePtr = PyUnicode_AsUTF8(dataTypeValue);

    if (fieldNamePtr != NULL)
    {
      std::string fieldType = dataTypePtr;
      field.field_type_ = vectordb::server::web::WebUtil::GetFieldType(fieldType);
    }
    else
    {
      PyErr_SetString(PyExc_TypeError, "invalid content: field type is not valid UTF8 string");
      return NULL;
    }

    Py_DECREF(dataTypeKey);
    Py_DECREF(dataTypeValue);

    if (field.field_type_ == vectordb::engine::meta::FieldType::VECTOR_DOUBLE ||
        field.field_type_ == vectordb::engine::meta::FieldType::VECTOR_FLOAT)
    {
      PyObject *dimensionsKey = PyUnicode_DecodeUTF8(tableSchemaKey_dimensions.c_str(), tableSchemaKey_dimensions.size(), "strict");
      PyObject *dimensionsValue = PyDict_GetItem(dict_obj, dimensionsKey);
      if (dimensionsValue == NULL)
      {
        PyErr_SetString(PyExc_TypeError, "invalid parameter: vector field has no dimension");
        return NULL;
      }
      if (PyLong_Check(dimensionsValue))
      {
        const long dimensions = PyLong_AsLong(dimensionsValue);
        field.vector_dimension_ = dimensions;
      }
      else
      {
        PyErr_SetString(PyExc_TypeError, "invalid parameter: dimension is not int");
        return NULL;
      }
      Py_DECREF(dimensionsKey);
      Py_DECREF(dimensionsValue);
      schema.fields_.push_back(field);
    }
    Py_DECREF(dict_obj);
  }

  Py_DECREF(tableFieldsListPtr);

  // TODO: add auto embedding here

  auto status = db->CreateTable(db_name, schema);
  if (!status.ok())
  {
    PyErr_SetString(PyExc_Exception, status.message().c_str());
    return NULL;
  }
  return PyLong_FromLong(int(status.code()));
}
static PyObject *insert(PyObject *self, PyObject *args, PyObject *kwargs)
{

  static const char *keywords[] = {"table_name", "records", NULL};
  const char *tableNamePtr;
  PyObject *recordListPtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO", (char **)keywords, &tableNamePtr, &recordListPtr))
    return NULL;

  std::string tableName = tableNamePtr;

  PyObject *json_module = PyImport_ImportModule("json");

  if (json_module == NULL)
  {
    PyErr_SetString(PyExc_Exception, "Unable to import json module");
    return NULL;
  }

  PyObject *dumps_func = PyObject_GetAttrString(json_module, "dumps");
  Py_DECREF(json_module);

  if (dumps_func == NULL || !PyCallable_Check(dumps_func))
  {
    PyErr_SetString(PyExc_Exception, "Unable to get address of json.dumps method");
    Py_XDECREF(dumps_func);
    return NULL;
  }

  PyObject *args_tuple = PyTuple_Pack(1, recordListPtr);
  PyObject *kwargs_dict = PyDict_New(); // You can pass keyword arguments here if needed

  PyObject *json_str = PyObject_Call(dumps_func, args_tuple, kwargs_dict);

  Py_DECREF(dumps_func);
  Py_DECREF(args_tuple);
  Py_DECREF(kwargs_dict);

  if (json_str == NULL)
  {
    PyErr_SetString(PyExc_Exception, "unable to dump records as JSON");
    return NULL;
  }

  // Convert the PyObject to a C-style string (UTF-8)
  const char *utf8_str = PyUnicode_AsUTF8(json_str);
  Py_DECREF(json_str);

  if (utf8_str == NULL)
  {
    return NULL;
  }

  auto records = vectordb::Json();
  records.LoadFromString(std::string(utf8_str));
  auto status = db->Insert(db_name, tableName, records);
  return PyLong_FromLong(int(status.code()));
}

static PyObject *query(PyObject *self, PyObject *args, PyObject *kwargs)
{
  static const char *keywords[] = {"table_name", "query_field", "query_vector", "limit", NULL};
  const char *tableNamePtr, *queryFieldPtr;
  int limit;
  PyObject *queryVector;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ssOi", (char **)keywords, &tableNamePtr, &queryFieldPtr, &queryVector, &limit))
  {
    return NULL;
  }
  Py_XINCREF(queryVector);
  auto queryFields = std::vector<std::string>();

  Py_ssize_t queryVectorSize = PyList_Size(queryVector);
  std::string tableName = tableNamePtr, queryField = queryFieldPtr;
  auto vectorArr = std::make_unique<float[]>(queryVectorSize);
  for (Py_ssize_t i = 0; i < queryVectorSize; ++i)
  {
    PyObject *elem = PyList_GetItem(queryVector, i);
    Py_XINCREF(elem);
    vectorArr[i] = PyFloat_AsDouble(elem);
    Py_XDECREF(elem);
  }
  Py_XDECREF(queryVector);
  auto result = vectordb::Json();
  auto status = db->Search(
      db_name,
      tableName,
      queryField,
      queryFields,
      queryVectorSize,
      vectorArr.get(),
      limit,
      result,
      // TODO: make it variable
      true);
  if (!status.ok())
  {
    PyErr_SetString(PyExc_Exception, status.message().c_str());
    return NULL;
  }

  // Import the json module
  PyObject *json_module = PyImport_ImportModule("json");
  if (json_module == NULL)
  {
    PyErr_SetString(PyExc_Exception, "Unable to import json module");
    return NULL;
  }

  // Get a reference to the json.loads function
  PyObject *loads_func = PyObject_GetAttrString(json_module, "loads");
  if (loads_func == NULL || !PyCallable_Check(loads_func))
  {
    PyErr_SetString(PyExc_Exception, "Unable to get address of json.loads method");
    Py_XDECREF(loads_func);
    return NULL;
  }
  PyObject *resultString = PyUnicode_FromString(result.DumpToString().c_str());
  PyObject *args_tuple = PyTuple_Pack(1, resultString);
  PyObject *kwargs_dict = PyDict_New(); // You can pass keyword arguments here if needed

  PyObject *response = PyObject_Call(loads_func, args_tuple, kwargs_dict);

  Py_XDECREF(loads_func);
  Py_XDECREF(args_tuple);
  Py_XDECREF(kwargs_dict);

  if (response == NULL)
  {
    PyErr_SetString(PyExc_Exception, "unable to json.loads response from string");
    return NULL;
  }

  PyObject *ret = PyTuple_Pack(2, PyLong_FromLong(long(status.code())), response);
  return ret;
}

static PyObject *drop_table(PyObject *self, PyObject *args, PyObject *kwargs)
{
  static const char *keywords[] = {"table_name", NULL};
  const char *tableNamePtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", (char **)keywords, &tableNamePtr))
    return NULL;

  auto tableName = std::string(tableNamePtr);
  if (tableName.empty())
  {
    PyErr_SetString(PyExc_Exception, "empty table name");
    return NULL;
  }

  auto status = db->DropTable(db_name, tableName);
  return PyLong_FromLong(status.code());
}

static PyObject *unload_db(PyObject *self, PyObject *args, PyObject *kwargs)
{
  static const char *keywords[] = {"db_name", NULL};
  const char *dbNamePtr;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", (char **)keywords, &dbNamePtr))
    return NULL;

  auto dbName = std::string(dbNamePtr);
  if (dbName.empty())
  {
    PyErr_SetString(PyExc_Exception, "empty db name");
    return NULL;
  }

  auto status = db->DropTable(db_name, dbName);
  return PyLong_FromLong(status.code());
}
