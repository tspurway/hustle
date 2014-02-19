#include "stdint.h"
#include "string.h"
#include "stdio.h"

static char *_val_for_vid(uint32_t *nodes, uint32_t *kids, uint32_t vid, char *rval) {
    char *curr = rval;
    uint32_t node = nodes[vid];
    uint32_t kid_offset = (uint32_t )0x00ffffff & node;
    uint32_t parent = kids[kid_offset++];

    if (parent) {
        curr = _val_for_vid(nodes, kids, parent, rval);
    }

    uint16_t *radix = (uint16_t *)&kids[kid_offset];
    char *radix_chars = (char *)(radix + 1);
    memcpy(curr, radix_chars, *radix);
    curr += *radix;
    return curr;
}


int value_for_vid(uint32_t *nodes, uint32_t *kids, uint32_t vid, char *result, size_t *rlen) {
    char *end = _val_for_vid(nodes, kids, vid, result);
    *rlen = end - result;
    return 0;
}


static uint32_t _find_binary(uint32_t *knodes, uint8_t kid_len, unsigned char selector) {
    int lower = 0;
    int upper = kid_len - 1;

    while (lower <= upper) {
        int mid = lower + ((upper - lower) / 2);

        // unpack the node - the high order byte is the selector for those children
        uint32_t knode = knodes[mid];
        unsigned char rselect = (unsigned char) (knode >> 24);
        uint32_t node = ((uint32_t)0x00ffffff) & knode;

        if (rselect == selector) {
            return node;
        }
        else if (rselect < selector) {
            lower = mid + 1;
        }
        else{
            upper = mid - 1;
        }
     }
    return 0;
}


static uint32_t _vid_for_value(uint32_t *nodes, uint32_t *kids, uint32_t vid, char *key, uint16_t key_len) {
    if (!key_len) {
        return vid;
    }

    uint32_t node = nodes[vid];
    uint8_t kid_len = (uint8_t)(node >> 24);
    uint32_t kid_offset = ((uint32_t )0x00ffffff & node) + 1;
    uint16_t *radix = (uint16_t *)&kids[kid_offset];
    uint16_t radix_len = *radix;
    uint16_t i;

    // we need to compare the radix to the key
    if (radix_len <= key_len) {
        char *radix_chars = (char *)(radix + 1);
        for (i = 0; i < radix_len; i++) {
            if (radix_chars[i] != key[i]) {
                return 0;
            }
        }

        // did we find the VID?
        if (radix_len == key_len) {
            return vid;
        }

        // we have a matching radix, take the 'rest' of the key and match with it's children
        char *selector = key + radix_len;
        uint16_t selector_len = key_len - radix_len;
        uint16_t width = 2 + radix_len;
        kid_offset += width / 4;
        if (width % 4) {
            kid_offset++;
        }
        uint32_t *knodes = kids + kid_offset;
        uint32_t knode = _find_binary(knodes, kid_len, (unsigned char)(*selector));
        if (knode) {
            return _vid_for_value(nodes, kids, knode, selector, selector_len);
        }
    }
    return 0;

}

int vid_for_value(uint32_t *nodes, uint32_t *kids, char *key, uint16_t key_len, uint32_t *vid) {
    uint32_t node = _vid_for_value(nodes, kids, 0, key, key_len);
    if (node) {
        *vid = node;
        return 0;
    }
    return -1;
}


static void _print_it(uint32_t *nodes, uint32_t *kids, uint32_t curr_node, unsigned char selector, int depth) {
    int i;
    uint32_t node = nodes[curr_node];
    uint8_t kid_len = (uint8_t)(node >> 24);
    uint32_t kid_offset = (uint32_t )0x00ffffff & node;
    uint32_t *kid = kids + kid_offset;
    uint32_t parent = kid[0];
    uint16_t radix_len = ((uint16_t *)kid)[2];
    for(i = 0; i < depth; ++i) {
        printf("   ");
    }
    if(radix_len > 0) {
        char *radix = ((char *)kid) + 6;
        printf("%d '%.*s' ", radix_len, radix_len, radix);
    }
    else {
        printf("<none> ");
    }
    printf("%d(%d) '%c'(0x%x) - %d\n", curr_node, parent, selector, selector, kid_len);

    // pad
    uint32_t child_offset = 6 + radix_len;
    child_offset += (4 - (child_offset % 4)) % 4;
    child_offset /= 4;

    // process kids
    uint32_t *children = kid + child_offset;
    for(i = 0; i < kid_len; ++i) {
        uint32_t child = children[i];
        unsigned char sel = (unsigned char)(child >> 24);
        uint32_t new_node = child & 0x00ffffff;
        _print_it(nodes, kids, new_node, sel, depth + 1);
    }
}

void print_it(uint32_t *nodes, uint32_t *kids) {
    _print_it(nodes, kids, 0, '\0', 0);
}


void summarize(uint32_t *nodes, uint32_t *kids, int num_nodes) {
    int i;
    printf("Summarize nodes=%p kids=%p num_nodes=%d\n", nodes, kids, num_nodes);
    for(i = 0; i < num_nodes; ++i) {
        uint32_t node = nodes[i];
        uint8_t kid_len = (uint8_t)(node >> 24);
        uint32_t kid_offset = (uint32_t )0x00ffffff & node;
        uint32_t *kid = kids + kid_offset;
        uint32_t parent = kid[0];
        uint16_t radix_len = ((uint16_t *)kid)[2];

        printf("%d %d | %d ", kid_len, kid_offset, parent);
        if(radix_len > 0) {
            char *radix = ((char *)kid) + 6;
            printf("%d '%.*s'\n", radix_len, radix_len, radix);
        }
        else {
            printf("0 ''\n");
        }
        //printf("%d %d(%d) - %d, %d\n", i, node, parent, kid_len, kid_offset);
    }
}