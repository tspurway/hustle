#include "clru.h"
#include "lru.h"
#include <iostream>
#include <vector>

using namespace std;

void
evict(char *k, void *unused, void *unused0)
{
    cerr << "Evicted: " << k << "_" << endl;
}

void
fn(char *x, void *unused, void *unused0)
{
    cerr << "Requested: " << x << endl;
}

int
main()
{
    CharLRU c(fn, evict, 5, NULL, NULL, NULL);
    for (int i = 0; i< 10; i++) {
        c.get( "Aello");
    }
    c.get("BI");
    c.set("CEllo");
    c.set("Di");
    c.set("Eello");

    for (int i = 0; i< 10; i++) {
        c.get( "Aello");
    }

    c.get("HI");
    c.get("KI");
    c.get("II");
    c.set("Eello");
    c.get("Eello");

    c.evictall();
    return 0;
}
