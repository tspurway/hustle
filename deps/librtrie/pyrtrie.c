#include <string.h>
#include <stdint.h>
#include <stdint.h>

#include "Python.h"
#include "rtrie.h"


#if PY_MAJOR_VERSION >= 3
    #define PYSTR_CREATE PyBytes_FromStringAndSize
#else
    #define PYSTR_CREATE PyString_FromStringAndSize
#endif

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
    typedef int Py_ssize_t;
    #define PY_SSIZE_T_MAX INT_MAX
    #define PY_SSIZE_T_MIN INT_MIN
#endif


static PyObject *
py_print_it(PyObject *self, PyObject *args)
{
    uint64_t node_ptr, kid_ptr;
    uint32_t *nodes;
    uint32_t *kids;

    if (!PyArg_ParseTuple(args, "K|K", &node_ptr, &kid_ptr))
        return NULL;

    nodes = (uint32_t *)node_ptr;
    kids = (uint32_t *)kid_ptr;

    print_it(nodes, kids);
    Py_XINCREF(Py_None);
    return Py_None;
}

static PyObject *
py_summarize(PyObject *self, PyObject *args)
{
    uint64_t node_ptr, kid_ptr;
    uint32_t size;
    uint32_t *nodes;
    uint32_t *kids;

    if (!PyArg_ParseTuple(args, "K|K|I", &node_ptr, &kid_ptr, &size))
        return NULL;

    nodes = (uint32_t *)node_ptr;
    kids = (uint32_t *)kid_ptr;

    summarize(nodes, kids, size);
    Py_XINCREF(Py_None);
    return Py_None;
}

static PyObject *
py_value_for_vid(PyObject *self, PyObject *args)
{
    uint64_t node_ptr, kid_ptr;
    uint32_t vid;
    uint32_t *nodes;
    uint32_t *kids;
    char res[8092];
    size_t rlen;

    if (!PyArg_ParseTuple(args, "K|K|I", &node_ptr, &kid_ptr, &vid))
        return NULL;

    nodes = (uint32_t *)node_ptr;
    kids = (uint32_t *)kid_ptr;

    if (!value_for_vid(nodes, kids, vid, res, &rlen)) {
        return PYSTR_CREATE(res, rlen);
    }
    Py_XINCREF(Py_None);
    return Py_None;
}

//TODO: these routines should return 0 on not found
// int vid_for_value(uint32_t *nodes, uint32_t *kids, char *key, uint16_t key_len, uint32_t *vid);
static PyObject *
py_vid_for_value(PyObject *self, PyObject *args)
{
    uint64_t node_ptr, kid_ptr;
    uint32_t vid;
    uint32_t *nodes;
    uint32_t *kids;
    char *key;
    Py_ssize_t key_len;

    if (!PyArg_ParseTuple(args, "K|K|s#", &node_ptr, &kid_ptr, &key, &key_len))
        return NULL;

    nodes = (uint32_t *)node_ptr;
    kids = (uint32_t *)kid_ptr;

    if (!vid_for_value(nodes, kids, key, key_len, &vid)) {
        return PyInt_FromLong((long)vid);
    }
    Py_XINCREF(Py_None);
    return Py_None;
}


PyDoc_STRVAR(module_doc, "Python wrapper for the rtrie.");

static PyMethodDef rtrie_methods[] = {

    {"value_for_vid", py_value_for_vid, METH_VARARGS,
        "Get Value based on VID"},
    {"vid_for_value", py_vid_for_value, METH_VARARGS,
        "Get VID based on Value"},
    {"print_it", py_print_it, METH_VARARGS,
        "Print rtrie"},
    {"summarize", py_summarize, METH_VARARGS,
        "Summarize rtrie"},

    {NULL, NULL, 0, NULL}
};


#if PY_MAJOR_VERSION <= 2

extern PyMODINIT_FUNC
initrtrie(void)
{
    PyObject *m;

    m = Py_InitModule3("rtrie", rtrie_methods, module_doc);

    if (m == NULL)
        return;
    PyModule_AddStringConstant(m, "__version__", MODULE_VERSION);
}

#else

/* Python 3.x */

static PyModuleDef rtrie_module = {
    PyModuleDef_HEAD_INIT,
    "rtrie",
    module_doc,
    -1,
    rtrie_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

extern PyMODINIT_FUNC
PyInit_rtrie(void)
{
    PyObject *m;

    m = PyModule_Create(&rtrie_module);
    if (m == NULL)
        goto finally;
    PyModule_AddStringConstant(m, "__version__", MODULE_VERSION);

finally:
    return m;
}

#endif
