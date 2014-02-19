/*
 *  Copyright (c) 2013, Chango Inc.
 *
 *  Based on the 'LRU cache implementation in C++' article by Tim Day.
 *  Copyright (c) 2010-2011, Tim Day <timday@timday.com>
 *
 *  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 *  WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 *  MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 *  ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 *  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 *  ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 *  OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef _CLRU_
#define _CLRU_

#include <cstdlib>
#include <cassert>
#include <list>
#include <map>
#include <cstring>

struct cmp_str {
    bool operator()(const char *a, const char *b) const {
        return strcmp(a, b) < 0;
    }
};

typedef void (*CharFunc)(char *, void *);

class CharLRU
{
    public:
        typedef std::list<char *> CharList;
        typedef std::map<char *, CharList::iterator, cmp_str> Map;
        CharLRU(CharFunc fetch, CharFunc evict, size_t c, void *cookie)
            :_fetch(fetch), _evict(evict), _capacity(c), _cookie(cookie)
        {
            assert(_capacity != 0);
        }

        void evictall() {
            unsigned long count = charlist.size();
            for (unsigned long i = 0; i < count; i++) {
                evict();
            }
        }

        void set(char *k) {
            touch(k, false);
        }

        void get(char *k) {
            touch(k, true);
        }
    private:
        void touch(char *_k, bool shouldFetch) {
            const Map::iterator it =locationMap.find(_k);

            if (it == locationMap.end()) {
                char *k = strdup(_k);
                assert(k != NULL);
                assert(strlen(_k) == strlen(k));
                if (shouldFetch) {
                    _fetch(k, _cookie);
                }
                insert(k);
            } else {
                charlist.splice(charlist.end(), charlist, it->second);
            }
        }

        void insert(char *k) {
            if (locationMap.size()==_capacity) 
                evict();
            CharList::iterator it =charlist.insert(charlist.end(), k);
            locationMap.insert(std::make_pair(k, it));
        }

        void evict() {
            assert(!charlist.empty());

            const Map::iterator it =locationMap.find(charlist.front());
            assert(it != locationMap.end());

            char *k = it->first;
            assert(k != NULL);
            _evict(k, _cookie);
            locationMap.erase(it);
            charlist.pop_front();
            free(k);
        }

        CharFunc _fetch, _evict;
        const size_t _capacity;
        void *_cookie;

        CharList charlist;
        Map locationMap;
};

typedef void (*IntFunc)(long, void *);
class IntLRU
{
    public:
        typedef std::list<long> IntList;
        typedef std::map<long, IntList::iterator> Map;
        IntLRU(IntFunc fetch, IntFunc evict, size_t c, void *cookie)
            :_fetch(fetch), _evict(evict), _capacity(c), _cookie(cookie)
        {
            assert(_capacity != 0);
        }

        void evictall() {
            unsigned long count = longlist.size();
            for (unsigned long i = 0; i < count; i++) {
                evict();
            }
        }

        void set(long k) {
            touch(k, false);
        }

        void get(long k) {
            touch(k, true);
        }
    private:
        void touch(long k, bool shouldFetch) {
            const Map::iterator it = locationMap.find(k);

            if (it == locationMap.end()) {
                if (shouldFetch) {
                    _fetch(k, _cookie);
                }
                insert(k);
            } else {
                longlist.splice(longlist.end(), longlist, it->second);
            }
        }

        void insert(long k) {
            if (locationMap.size()==_capacity) 
                evict();
            IntList::iterator it =longlist.insert(longlist.end(), k);
            locationMap.insert(std::make_pair(k, it));
        }

        void evict() {
            assert(!longlist.empty());

            const Map::iterator it =locationMap.find(longlist.front());
            assert(it != locationMap.end());

            long k = it->first;
            _evict(k, _cookie);
            locationMap.erase(it);
            longlist.pop_front();
        }

        IntFunc _fetch, _evict;
        const size_t _capacity;
        void *_cookie;

        IntList longlist;
        Map locationMap;
};
#endif
