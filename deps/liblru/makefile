##############################################################################
#  Copyright (c) 2010-2011, Tim Day <timday@timday.com>                      #
#                                                                            #
#  Permission to use, copy, modify, and/or distribute this software for any  #
#  purpose with or without fee is hereby granted, provided that the above    #
#  copyright notice and this permission notice appear in all copies.         #
#                                                                            #
#  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES  #
#  WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF          #
#  MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR   #
#  ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES    #
#  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN     #
#  ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF   #
#  OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.            #
##############################################################################

all:		main main1

CACHE_HEADERS=$(wildcard ./*cache*.h)

CXX = c++
CXXFLAGS = -g3 -O0 -Wall -I .
CXXOPTIM = -march=native -O3 -DNDEBUG

main:	$(CACHE_HEADERS) main.cc
		$(CXX) $(CXXFLAGS) -o main main.cc

main1:	$(CACHE_HEADERS) main1.cc
		$(CXX) $(CXXFLAGS) -o main1 main1.cc

lru:	lru.cc
	cc -c $(CXXFLAGS) lru.cc -o lru.o

clean:		
		rm -r -f main1 main *.dSYM

.PHONY:		all publish clean

