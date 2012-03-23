#include <Python.h>

static PyMethodDef UussPBMethods[] = {
  {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
inituuss_pb(void)
{
  PyObject *m;

  m = Py_InitModule("uuss_pb", UussPBMethods);
  if (m == NULL)
    return;
}
