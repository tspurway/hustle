/* clzf - the liblzf wrapper. 
 * All rights reserved by Chango Inc.
 
 * clzf tries to compresses input data(say its length is L) by liblzf. If it 
 * fails to reach the expected compression level (L - 4), it dumps the raw data
 * directly. */

#include <string.h>
#include <stdint.h>

#include "Python.h"
#include "lzf.h"


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

/*
 * The output format of compressed data has two sections,
 *    
 *  | header | payload |
 *
 * Where the header has two layouts which depend on the following payload section
 * is compressed or not.
 *
 * 1). If payload is compressed by lzf, the header is
 *  
 *  | 01000000 | clen | len | payload |
 *
 *  where, first byte is flag as CLZF_ENCVAL, followed by length of compressed 
 *  data and length of input data. All lengthes are encoded as 1 byte, 2 bytes
 *  or 4 bytes, see following length encoding part for details.
 *
 * 2). Otherwise, dump the original data
 *
 *  | len | data |
 *
 *  where, len is the length of input data.
 */

/*
 * Length Encoding
 *
 * To save more space, length is encoded to 1 byte, 2 bytes or 5 bytes.
 * In the first byte, the mose significant 2 bits are used to store length encode
 * codes. If length is greater than 14 bits, 5 bytes would be taken(one byte for
 * flag CLZF_32BITLEN, the other 4 for the actual length).
 */

#define CLZF_6BITLEN 0     /* 6 bits length */
#define CLZF_14BITLEN 1    /* 14 bites length */
#define CLZF_32BITLEN 2    /* 32 bites full length followed by the 1st byte */
#define CLZF_ENCVAL 3      /* data section has been encoded by liblzf */

/* 
 * The Max Length of header section
 *
 * 1 byte for header, two 5(1 + 4) bytes for the lengthes
 */
#define CLZF_MAXHEADER_LEN (1 + 5 + 5)

/*
 * Length in Bytes
 *
 * Calculate how many bytes would be taken by the length
 */
#define LEN_IN_BYTES(l) (((l) < (1 << 6))? \
                        1:((l) < (1 << 14)? 2:5))

/*
 * Save length info, and encode the MSB two bits
 */
static uint32_t
save_len(char *io, uint32_t len)
{
    unsigned char *buf = (unsigned char*)io;
    uint32_t nwritten;

    if (len < (1 << 6)) {
        *buf = ((len & 0xFF) | (CLZF_6BITLEN << 6));
        nwritten = 1;
    } else if (len < (1 << 14)) { 
        *buf = ((len >> 8) & 0xFF) | (CLZF_14BITLEN << 6);
        *(buf + 1) = len & 0xFF;
        nwritten = 2;
    } else {
        *buf++ = (CLZF_32BITLEN << 6);
        buf[0] = len & 0xFF;
        buf[1] = (len >> 8) & 0xFF;
        buf[2] = (len >> 16) & 0xFF;
        buf[3] = (len >> 24) & 0xFF;
        nwritten = 1 + 4;
    }
    return nwritten;
}

/*
 * Load length info, and check length is encoded by liblzf
 */
static uint32_t
load_len(char *buf, int *isencoded)
{
    unsigned char *io = (unsigned char*)buf;
    int type;

    if (isencoded) *isencoded = 0;
    type = (io[0] & 0xC0) >> 6;
    if (type == CLZF_ENCVAL) {
        if (isencoded) *isencoded = 1;
        return io[0] & 0x3F;
    } else if (type == CLZF_6BITLEN) {
        return io[0] & 0x3F;
    } else if (type == CLZF_14BITLEN) {
        return ((io[0] & 0x3F) << 8) | io[1];
    } else {
        io++;
        return io[0] | (io[1] << 8) | (io[2] << 16) | (io[3] << 24);
    }
}

static PyObject *
py_compress(PyObject *self, PyObject *args)
{
    char *input, *output, *buf;
    long len;
    uint32_t outlen, offset, nwritten;
    Py_ssize_t inlen;
    PyObject *result;

    if (!PyArg_ParseTuple(args, "s#", &input, &inlen))
        return NULL;

    len = Py_SAFE_DOWNCAST(inlen, Py_ssize_t, int);
    buf = (char *)PyMem_Malloc(len + CLZF_MAXHEADER_LEN + 1);
    if (buf == NULL) {
        return PyErr_NoMemory();
    }
    /* if size of data is less than 20 bytes, skip the compression.
     * dump it directly */
    if (len < 20) {
        goto dumpraw; 
    }
    /* try to compress to original size - 4, otherwise compression is useless*/
    outlen = (uint32_t)(len - 4);
    output = buf + CLZF_MAXHEADER_LEN;
    outlen = lzf_compress(input, (uint32_t)len, output, outlen);
    if (outlen) {
        /* all lengthes are available, get the offset now */
        offset = CLZF_MAXHEADER_LEN - LEN_IN_BYTES(outlen) - LEN_IN_BYTES(len) - 1; 
        *(buf + offset) = (CLZF_ENCVAL << 6);
        nwritten = 1;
        nwritten += save_len(buf + offset + nwritten, outlen);
        nwritten += save_len(buf + offset + nwritten, (uint32_t)len);
        result = PYSTR_CREATE(buf + offset, nwritten + outlen);
        PyMem_Free(buf);
        return result;
    }
    
dumpraw:
    nwritten = save_len(buf, (uint32_t)len);
    memcpy(buf + nwritten, input, len);
    result = PYSTR_CREATE(buf, nwritten + len);
    PyMem_Free(buf);

    return result; 
}


static PyObject *
py_decompress(PyObject *self, PyObject *args)
{
    char *input, *output;
    long outlen;
    int isencoded;
    uint32_t len, clen, offset;
    Py_ssize_t inlen;
    PyObject *result;

    if (!PyArg_ParseTuple(args, "s#", &input, &inlen))
        return NULL;

    len = load_len(input, &isencoded);
    if (!isencoded) {
        result = PYSTR_CREATE(input + LEN_IN_BYTES(len), len);
        return result;
    } else {
        if (len != 0) {
            PyErr_SetString(PyExc_ValueError, "Invalid input");
            return NULL; 
        }
        offset = 1;
        len = load_len(input + offset, NULL);
        clen = len;
        offset += LEN_IN_BYTES(len);
        len = load_len(input + offset, NULL);
        offset += LEN_IN_BYTES(len);
        result = PYSTR_CREATE(NULL, len);
        output = PyString_AS_STRING(result);
        if (output == NULL) {
            return PyErr_NoMemory();
        }
        outlen = lzf_decompress(input + offset, clen, output, len);
        if (outlen) {
            return result;
        } else {
            PyErr_SetString(PyExc_ValueError, "Invalid input");
            Py_CLEAR(result);
            return result;
        } 
    }
}


PyDoc_STRVAR(module_doc, "Python wrapper for the liblzf.");

static PyMethodDef clzf_methods[] = {

    {"compress", py_compress, METH_VARARGS,
        "Compressed by liblzf"},
    {"decompress", py_decompress, METH_VARARGS,
        "Decompressed by liblzf"},

    {NULL, NULL, 0, NULL}
};


#if PY_MAJOR_VERSION <= 2

extern PyMODINIT_FUNC
initclzf(void)
{
    PyObject *m;

    m = Py_InitModule3("clzf", clzf_methods, module_doc);

    if (m == NULL)
        return;
    PyModule_AddStringConstant(m, "__version__", MODULE_VERSION);
}

#else

/* Python 3.x */

static PyModuleDef clzf_module = {
    PyModuleDef_HEAD_INIT,
    "clzf",
    module_doc,
    -1,
    clzf_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

extern PyMODINIT_FUNC
PyInit_clzf(void)
{
    PyObject *m;

    m = PyModule_Create(&clzf_module);
    if (m == NULL)
        goto finally;
    PyModule_AddStringConstant(m, "__version__", MODULE_VERSION);

finally:
    return m;
}

#endif
