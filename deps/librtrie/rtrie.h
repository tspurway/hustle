#ifndef _RTRIE_H_
#define _RTRIE_H_

#include "stdint.h"
#include "stddef.h"

int value_for_vid(uint32_t *nodes, uint32_t *kids, uint32_t vid, char *result, size_t *rlen);
int vid_for_value(uint32_t *nodes, uint32_t *kids, char *key, uint16_t key_len, uint32_t *vid);
void print_it(uint32_t *nodes, uint32_t *kids);
void summarize(uint32_t *nodes, uint32_t *kids, int num_nodes); 

#endif
