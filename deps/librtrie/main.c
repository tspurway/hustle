d#include <stdio.h>
#include "rtrie.h"

uint8_t nodes[] = {0, 0, 0, 2, 16, 0, 0, 1, 11, 0, 0, 2, 19, 0, 0, 0, 4, 0, 0, 1, 8, 0, 0, 0, 22, 0, 0, 0};
uint8_t kids[] = {0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 103, 2, 0, 0, 104, 0, 0, 0, 0, 4, 0, 103, 111, 111, 100,
        0, 0, 5, 0, 0, 98, 4, 0, 0, 0, 3, 0, 98, 121, 101, 0, 0, 0, 0, 0, 0, 0, 4, 0, 104, 101, 108, 108, 0,
        0, 1, 0, 0, 111, 6, 0, 0, 115, 2, 0, 0, 0, 1, 0, 111, 0, 3, 0, 0, 116, 1, 0, 0, 0, 5, 0, 116, 104, 101,
        114, 101, 0, 2, 0, 0, 0, 4, 0, 115, 105, 110, 107, 0, 0};

int main(int argc, const char * argv[])
{
    char result[256];
    size_t rlen;
    uint32_t *node32s = (uint32_t*)nodes;
    uint32_t *kid32s = (uint32_t*)kids;

    printf("testing value_for_vid()...\n");
    int x = value_for_vid(node32s, kid32s, 3, result, &rlen);
    result[rlen] = '\0';
    printf("Done rval=%d %s\n", x, result);
    x = value_for_vid(node32s, kid32s, 1, result, &rlen);
    result[rlen] = '\0';
    printf("Done rval=%d %s\n", x, result);
    x = value_for_vid(node32s, kid32s, 4, result, &rlen);
    result[rlen] = '\0';
    printf("Done rval=%d %s\n", x, result);

    printf("testing vid_for_value()...\n");
    uint32_t vid=0;
    x = vid_for_value(node32s, kid32s, "hello", 5, &vid);
    printf("Done vid=%d %d\n", vid, x);

    x = vid_for_value(node32s, kid32s, "hellothere", 10, &vid);
    printf("Done vid=%d %d\n", vid, x);

    x = vid_for_value(node32s, kid32s, "good", 4, &vid);
    printf("Done vid=%d %d\n", vid, x);

    x = vid_for_value(node32s, kid32s, "dung", 4, &vid);
    printf("Done vid=%d %d\n", vid, x);

    return 0;
}

