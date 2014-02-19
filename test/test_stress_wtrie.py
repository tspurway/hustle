import unittest
import os
from wtrie import Trie
from rtrie import value_for_vid, vid_for_value


pwd = os.getcwd()
if os.path.basename(pwd) != 'test':
    fixture = os.path.join(pwd, 'test/fixtures/keys')
else:
    fixture = os.path.join(pwd, 'fixtures/keys')


class TestStressWTrie(unittest.TestCase):
    def test_stress_wtrie(self):
        ktrie = Trie()
        strie = Trie()
        etrie = Trie()

        keywords = {}
        search_terms = {}
        exchange_ids = {}

        with open(fixture) as f:
            for data in f:
                for word in data.split(' '):
                    vid = ktrie.add(word)
                    actual_vid = keywords.get(word)
                    if actual_vid is not None:
                        self.assertEqual(vid, actual_vid)
                    else:
                        keywords[word] = vid

                vid = strie.add(data)
                actual_vid = search_terms.get(data)
                if actual_vid is not None:
                    self.assertEqual(vid, actual_vid)
                else:
                    search_terms[data] = vid

        nodes, kids, nodelen = etrie.serialize()
        naddr, nlen = nodes.buffer_info()
        kaddr, klen = kids.buffer_info()
        #summarize(naddr, kaddr, nodelen)
        #print_it(naddr, kaddr)

        for dc, vid in exchange_ids.iteritems():
            rvid = etrie.add(dc)
            self.assertEqual(vid, rvid)

            print dc, vid
            value = value_for_vid(naddr, kaddr, vid)
            self.assertEqual(dc, value)
            if dc != value:
                print "      dc=%s adc=%s" % (dc, value)

            avid = vid_for_value(naddr, kaddr, dc)
            #print "vid=%s avid=%s" % (vid, avid)
            self.assertEqual(vid, avid)
