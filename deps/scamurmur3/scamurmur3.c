
// License: MIT License
// http://www.opensource.org/licenses/mit-license.php

// SMHasher code is from SMHasher project, authored by Austin Appleby, et al.
// http://code.google.com/p/smhasher/

// Python extension code by Patrick Hensley
// Ported from C++ to C by Chango Corp.


#include <Python.h>
#include "murmur3.h"


#if PY_VERSION_HEX < 0x02050000
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif


static PyObject *
_py_murmur3_128(PyObject *self, PyObject *args, int x86, int size)
{
    const char *key;
    Py_ssize_t len;
    uint32_t seed = 0;
    unsigned char out[16];

    if (!PyArg_ParseTuple(args, "s#|I", &key, &len, &seed)) {
        return NULL;
    }

    if (x86) {
        MurmurHash3_x86_128((void *)key, len, seed, &out);
    } else {
        MurmurHash3_x64_128((void *)key, len, seed, &out);
    }

    return _PyLong_FromByteArray((const unsigned char *)&out, size, 0, 0);
}

static PyObject *
py_murmur3_x86_32(PyObject *self, PyObject *args)
{
    const char *key;
    Py_ssize_t len;
    uint32_t seed = 0;
    unsigned char out[4];

    if (!PyArg_ParseTuple(args, "s#|I", &key, &len, &seed)) {
        return NULL;
    }

    MurmurHash3_x86_32((void *)key, len, seed, &out);

    return _PyLong_FromByteArray((const unsigned char *)&out, 4, 0, 0);
}

static PyObject *
py_murmur3_x86_64(PyObject *self, PyObject *args)
{
    return _py_murmur3_128(self, args, 1, 8);
}


static PyObject *
py_murmur3_x64_64(PyObject *self, PyObject *args)
{
    return _py_murmur3_128(self, args, 0, 8);
}


static PyObject *
py_murmur3_x86_128(PyObject *self, PyObject *args)
{
    return _py_murmur3_128(self, args, 1, 16);
}


static PyObject *
py_murmur3_x64_128(PyObject *self, PyObject *args)
{
    return _py_murmur3_128(self, args, 0, 16);
}


PyDoc_STRVAR(module_doc, "Python wrapper for the SMHasher routines.");

static PyMethodDef scamurmur3_methods[] = {
    {"murmur3_x86_32", py_murmur3_x86_32, METH_VARARGS,
        "Make an x86 murmur3 32-bit hash value"},

    {"murmur3_x86_64", py_murmur3_x86_64, METH_VARARGS,
        "Make an x86 murmur3 64-bit hash value"},
    {"murmur3_x64_64", py_murmur3_x64_64, METH_VARARGS,
        "Make an x64 murmur3 64-bit hash value"},

    {"murmur3_x86_128", py_murmur3_x86_128, METH_VARARGS,
        "Make an x86 murmur3 128-bit hash value"},
    {"murmur3_x64_128", py_murmur3_x64_128, METH_VARARGS,
        "Make an x64 murmur3 128-bit hash value"},

    {NULL, NULL, 0, NULL}
};


#if PY_MAJOR_VERSION <= 2

extern PyMODINIT_FUNC
initscamurmur3(void)
{
    PyObject *m;

    m = Py_InitModule3("scamurmur3", scamurmur3_methods, module_doc);

    if (m == NULL)
        return;
    PyModule_AddStringConstant(m, "__version__", MODULE_VERSION);
}

#else

/* Python 3.x */

static PyModuleDef scamurmur3_module = {
    PyModuleDef_HEAD_INIT,
    "scamurmur3",
    module_doc,
    -1,
    scamurmur3_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

extern PyMODINIT_FUNC
PyInit_smhasher(void)
{
    PyObject *m;

    m = PyModule_Create(&smhasher_module);
    if (m == NULL)
        goto finally;
    PyModule_AddStringConstant(m, "__version__", MODULE_VERSION);

finally:
    return m;
}

#endif

