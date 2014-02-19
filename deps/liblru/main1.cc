#include "clru.h"
#include "lru.h"
#include <iostream>
#include <vector>
#include <cassert>

using namespace std;

void
evict(char *k, void *unused, void *unused0)
{
    cout << "Evicted: " << k << "_" << endl;
}

void
fn(char *x, void *unused, void *unused0)
{
    cout << "Requested: " << x << endl;
}

static char *arr1[] = {"12", "17", "18", "19", "19", "19", "17", "17", "18", "18", "18",};
static char *arr2[] = {"123", "123", "123", "123", "123", "123", "223",};
static char *arr3[] = {"12", "17", "18", "19", "19", "19", "17", "17", "18", "18", "18",
                       "12", "17", "18", "19", "19", "19", "17", "17", "18", "18", "18",};

static char *arr4[] = {"11", "17", "18", "19", "15", "19", "17", "11", "18", "18", "18",
                       "12", "17", "18", "19", "16", "19", "15", "17", "18", "11", "11",
                       "12", "15", "16", "19", "19", "16", "17", "17", "18", "11", "18",
                       "12", "17", "15", "11", "19", "19", "17", "11", "18", "15", "18",};


static void tester(char **x, int count) {
    CharLRU c(fn, evict, 5, NULL, NULL, NULL);
    for (int i = 0; i< count; i++) {
        c.get(x[i]);
    }
    c.evictall();
}

int
main()
{
    tester(arr1, sizeof(arr1) / sizeof(arr1[0]));
    tester(arr2, sizeof(arr2) / sizeof(arr2[0]));
    tester(arr3, sizeof(arr3) / sizeof(arr3[0]));
    tester(arr4, sizeof(arr4) / sizeof(arr4[0]));
    return 0;
}
