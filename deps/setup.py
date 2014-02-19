from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext


__version__ = '"0.3.0"'


scamurmur3 = Extension(
    'scamurmur3',
    sources=[
        'scamurmur3/scamurmur3.c',
        'scamurmur3/murmur3.c',
    ],
    include_dirs=['./scamurmur3'],
    define_macros=[('MODULE_VERSION', __version__)]
)

clzf = Extension(
    'clzf',
    sources=[
        'liblzf/clzf.c',
        'liblzf/lzf_c.c',
        'liblzf/lzf_d.c',
    ],
    include_dirs=['./liblzf'],
    define_macros=[('MODULE_VERSION', __version__)]
)

rtrie = Extension(
    'rtrie',
    sources=[
        'librtrie/pyrtrie.c',
        'librtrie/rtrie.c',
    ],
    include_dirs=['./librtrie'],
    define_macros=[('MODULE_VERSION', __version__)]
)

wtrie = Extension(
    "wtrie",
    sources=["libwtrie/wtrie.pyx"],
    define_macros=[('MODULE_VERSION', __version__)]
)

clz4 = Extension(
    'clz4',
    sources=[
        'liblz4/clz4.c',
        'liblz4/lz4.c',
        'liblz4/lz4hc.c',
    ],
    include_dirs=['./liblz4'],
    define_macros=[('MODULE_VERSION', __version__)]
)

cardunion = Extension(
    "cardunion",
    ["cardunion/cardunion.pyx"],
    libraries=["m"],
    define_macros=[('MODULE_VERSION', __version__)]
)

ebitset = Extension(
    "pyebset",
    sources=["libebset/pyebset.pyx"],
    include_dirs=['./libebset'],
    language="c++",
    define_macros=[('MODULE_VERSION', __version__)]
)

maxhash = Extension(
    "maxhash",
    sources=["maxhash/maxhash.pyx"],
    define_macros=[('MODULE_VERSION', __version__)]
)

lru = Extension(
    "pylru",
    sources=["liblru/pylru.pyx"],
    include_dirs=['./liblru'],
    language="c++",
    define_macros=[('MODULE_VERSION', __version__)]
)


lmdb = Extension(
    "mdb",
    sources=["liblmdb/db.pyx", ],
    libraries=["lmdb"],
    library_dirs=["/usr/local/lib"],
    include_dirs=["/usr/local/include"],
    runtime_library_dirs=["/usr/local/lib"])


setup(
    name = "hustle-deps",
    version = __version__,
    cmdclass = {'build_ext': build_ext},
    description=('Hustle-deps: a collection of dependent libraries.'),
    author = 'Chango Inc.',
    license = 'MIT',
    ext_modules = [
        scamurmur3,
        cardunion,
        ebitset,
        maxhash,
        clzf,
        clz4,
        rtrie,
        wtrie,
        lru,
        lmdb,
    ]
)
