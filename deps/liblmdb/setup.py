from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

__version__ = '0.4.0'

setup(
    name = "mdb",
    version = __version__,
    description = 'Python client of MDB-Lightning',
    cmdclass = {'build_ext': build_ext},
    author = 'Chango Inc.',
    keywords=['mdb-ligtning', 'mdb', 'lmdb', 'key-value store'],
    license='MIT',
    ext_modules = [Extension("mdb", ["db.pyx", ],
                             libraries=["lmdb"],
                             library_dirs=["/usr/local/lib"],
                             include_dirs=["/usr/local/include"],
                             runtime_library_dirs=["/usr/local/lib"])]
)
