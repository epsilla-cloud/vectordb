#define PY_SSIZE_T_CLEAN
#include <string>
#include <Python.h>
#include "bindings/python/interface.h"
#include "db/db_server.hpp"
#include "server/web_server/web_controller.hpp"

const std::string tableSchemaKey_name = "name",
                  tableSchemaKey_dataType = "dataType",
                  tableSchemaKey_dimensions = "dimensions";

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
static PyObject *create_table(PyObject *self, PyObject *args)
{
  const char *tableNamePtr;
  PyObject *tableFieldsListPtr;

  if (!PyArg_ParseTuple(args, "sO", &tableNamePtr, &tableFieldsListPtr))
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

  auto status = db->CreateTable(db_name, schema);
  if (!status.ok())
  {
    PyErr_SetString(PyExc_Exception, status.message().c_str());
    return NULL;
  }
  return PyLong_FromLong(int(status.code()));
}
static PyObject *insert(PyObject *self, PyObject *args) {}
static PyObject *query(PyObject *self, PyObject *args) {}
