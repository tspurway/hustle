import unittest
import ujson
import os
import rtrie
import mdb
from pyebset import BitSet
from hustle.core.marble import Marble, MarbleStream
import clz4

_FIELDS_RAW = ("id", "name", "artist", "date", "quantity", "genre", "rating")
_FIELDS = ("+@4id", "+*name", "+$date", "+%2genre", "+@2rating", "artist", "@4quantity")
_PARTITIONS = "date"
_ALBUMS = [
    (1000, "Metallica", "Metallica", "1992-10-03", 5000000, "R&R", 5),
    (1001, "Reload", "Metallica", "1992-10-03", 2000000, "R&R", 4),
    (1002, "Use Your Imagination", "Guns&Roses", "1992-10-03", 4000000, "R&R", 5),
    (1003, "Come As You Are", "Nirvana", "1992-10-03", 6000000, "R&R", 5),
    (1004, "Pianist", "Ennio Morricone", "1986-01-03", 200000, "SoundTrack", 5),
    (1005, "The Good, The Bad, and The Ugly", "Ennio Morricone", "1986-01-03", 400000, "SoundTrack", 5),
    (1006, "Once Upon A Time In America", "Ennio Morricone", "1986-01-03", 1200000, "SoundTrack", 5),
    (1007, "Cinema Paradise", "Ennio Morricone", "1986-01-03", 50000, "SoundTrack", 4),
    (1008, "Taxi Driver", "Ennio Morricone", "1986-01-03", 300000, "SoundTrack", 5),
    (1009, "Once Upon A Time In The West", "Ennio Morricone", "1986-01-03", 1200000, "SoundTrack", 3),
]
_NPARTITIONS = 2  # Only two unique values for date


class TestMarble(unittest.TestCase):
    def setUp(self):
        self.albums = [dict(zip(_FIELDS_RAW, album)) for album in _ALBUMS]
        self.marble = Marble(name="Collections",
                             fields=_FIELDS,
                             partition=_PARTITIONS)
        self.n_inserted, self.files = self.marble._insert([(ujson.dumps(l) for l in self.albums)])

    def tearDown(self):
        for date, file in self.files.iteritems():
            os.unlink(file)

    def test_field_names(self):
        self.assertListEqual(sorted(list(_FIELDS_RAW)), sorted(self.marble._field_names))

    def test_marble_insert(self):
        #  test general infomation
        self.assertEqual(self.n_inserted, len(_ALBUMS))
        self.assertEqual(_NPARTITIONS, len(self.files))
        part_id = {}
        #  test that each sub db is fine
        for date, file in self.files.iteritems():
            env, txn, dbs, meta = self.marble._open(file)
            #  check meta db
            self.assertTrue(meta.contains(txn, "_vid_nodes"))
            self.assertTrue(meta.contains(txn, "_vid_kids"))
            self.assertTrue(meta.contains(txn, "_vid16_nodes"))
            self.assertTrue(meta.contains(txn, "_vid16_kids"))
            self.assertEqual(meta.get(txn, "name"), ujson.dumps("Collections"))
            self.assertEqual(meta.get(txn, "partition"), ujson.dumps("date"))
            self.assertEqual(meta.get(txn, "fields"), ujson.dumps(_FIELDS))
            vid_nodes, _ = meta.get_raw(txn, '_vid_nodes')
            vid_kids, _ = meta.get_raw(txn, '_vid_kids')
            vid16_nodes, _ = meta.get_raw(txn, '_vid16_nodes', (None, 0))
            vid16_kids, _ = meta.get_raw(txn, '_vid16_kids', (None, 0))
            #  check subdb, subinddb
            part_id[date] = 1
            for name, (db, ind_db, _, column, _) in dbs.iteritems():
                if name == "_count":
                    continue
                bitmaps = {}
                part_id[date] = 1
                for album in self.albums:
                    if date == album[_PARTITIONS]:  # match the partition
                        value = album[name]
                        i = part_id[album[_PARTITIONS]]
                        part_id[album[_PARTITIONS]] += 1
                        if column.is_trie:
                            if column.rtrie_indicator == mdb.MDB_UINT_16:
                                val = rtrie.vid_for_value(vid16_nodes, vid16_kids, value)
                            else:
                                val = rtrie.vid_for_value(vid_nodes, vid_kids, value)
                        elif column.is_lz4:
                            val = clz4.compress(value)
                        else:
                            val = value
                        # self.assertEqual(db.get(txn, i), val)
                        if ind_db is not None:
                            #  row_id should be in bitmap too
                            if val in bitmaps:
                                bitmap = bitmaps[val]
                            else:
                                bitmap = BitSet()
                                bitmap.loads(ind_db.get(txn, val))
                                bitmaps[val] = bitmap
                            self.assertTrue(i in bitmap)
            txn.commit()
            env.close()

    def test_marble_stream_get(self):
        for date, file in self.files.iteritems():
            stream = MarbleStream(file)
            rowid = 1
            for album in self.albums:
                if album[_PARTITIONS] != date:
                    continue
                # test 'get' first
                for k, v in album.iteritems():
                    self.assertEqual(v, stream.get(k, rowid))
                rowid += 1
            stream.close()

    def test_marble_stream_bit_ops(self):
        stream = MarbleStream(self.files["1992-10-03"])
        rowid = 1
        # test "name" index
        for album in self.albums:
            if album[_PARTITIONS] != "1992-10-03":
                continue
            bitset = stream.bit_eq("name", album["name"])
            bs = BitSet()
            bs.set(rowid)
            rowid += 1
            for i in bitset:
                self.assertTrue(i in bs)
        # test "genre" index
        bitset = stream.bit_eq("genre", "R&R")
        bs = BitSet()
        for i in range(1, 5):
            bs.set(i)
        for i in bitset:
            self.assertTrue(i in bs)

        stream.close()

        stream = MarbleStream(self.files["1986-01-03"])
        rowid = 1
        # test "name" index
        for album in self.albums:
            if album[_PARTITIONS] != "1986-01-03":
                continue
            bitset = stream.bit_eq("name", album["name"])
            bs = BitSet()
            bs.set(rowid)
            rowid += 1
            for i in bitset:
                self.assertTrue(i in bs)
        # test "genre" index
        bitset = stream.bit_eq("genre", "SoundTrack")
        bs = BitSet()
        for i in range(1, 7):
            bs.set(i)
        for i in bitset:
            self.assertTrue(i in bs)

        # test "rating" index
        # test for eq and not-eq
        bitset = stream.bit_eq("rating", 4)
        bs = BitSet()
        bs.set(4)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_eq("rating", 3)
        bs = BitSet()
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_eq("rating", 5)
        bs = BitSet()
        for i in range(1, 4):
            bs.set(i)
        bs.set(5)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_ne("rating", 5)
        bs = BitSet()
        bs.set(4)
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_ne("rating", 3)
        bs = BitSet()
        for i in range(1, 6):
            bs.set(i)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_ne("rating", 4)
        bs = BitSet()
        for i in range(1, 4):
            bs.set(i)
        bs.set(5)
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        # test "rating" index
        # test for eq_ex and not_eq_ex
        bitset = stream.bit_eq_ex("rating", [3, 4])
        bs = BitSet()
        bs.set(4)
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_eq_ex("rating", [5])
        bs = BitSet()
        for i in range(1, 4):
            bs.set(i)
        bs.set(5)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_ne_ex("rating", [5])
        bs = BitSet()
        bs.set(4)
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_ne_ex("rating", [3, 4])
        bs = BitSet()
        for i in range(1, 4):
            bs.set(i)
        bs.set(5)
        for i in bitset:
            self.assertTrue(i in bs)

        # test for less_than and less_eq
        bitset = stream.bit_ge("rating", 3)
        bs = BitSet()
        for i in range(1, 7):
            bs.set(i)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_gt("rating", 3)
        bs = BitSet()
        for i in range(1, 6):
            bs.set(i)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_le("rating", 3)
        bs = BitSet()
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_lt("rating", 3)
        bs = BitSet()
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_lt("rating", 5)
        bs = BitSet()
        bs.set(4)
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_le("rating", 5)
        bs = BitSet()
        for i in range(1, 7):
            bs.set(i)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_gt("rating", 5)
        bs = BitSet()
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_ge("rating", 5)
        bs = BitSet()
        for i in range(1, 4):
            bs.set(i)
        bs.set(5)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_le("rating", 4)
        bs = BitSet()
        bs.set(4)
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_lt("rating", 4)
        bs = BitSet()
        bs.set(6)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_ge("rating", 4)
        bs = BitSet()
        for i in range(1, 6):
            bs.set(i)
        for i in bitset:
            self.assertTrue(i in bs)

        bitset = stream.bit_gt("rating", 4)
        bs = BitSet()
        for i in range(1, 4):
            bs.set(i)
        bs.set(5)
        for i in bitset:
            self.assertTrue(i in bs)

        stream.close()
