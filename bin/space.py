#!/usr/bin/env python

from disco.ddfs import DDFS
from disco.util import urlsplit
import subprocess
import random
from tempfile import mkstemp
import os
import sys


def _find_tag_tree(host_tags, host, tag):
    if host not in host_tags:
        host_tags[host] = {}
    kids = host_tags[host]
    blobs = []
    for tag_frag in tag.split(':'):
        if tag_frag:
            if tag_frag not in kids:
                kids[tag_frag] = ({}, [])
            kids, blobs = kids[tag_frag]
    return kids, blobs


def _calculate_tag_tree(host, tag_tree, tag_sizes, tag_parts):
    total_tree_size = 0
    print "\n%s" % host,
    for tag_frag, (kids, blobs) in tag_tree.iteritems():
        # calc size of my blobs
        standard_output = None
        tag_frag_size = 0
        try:
            new_tag_parts = tag_parts + [tag_frag]
            tag_prefix = ":".join(new_tag_parts)
            if len(blobs):
                (fd, standard_output) = mkstemp()
                cmd = "du -m " + " ".join(blobs)
                # print "OUT: %s" % repr(cmd)
                sys.stdout.flush()
                subprocess.Popen(["ssh", host, cmd], stdout=fd,).communicate()
                with open(standard_output) as f:
                    for line in f:
                        print ".",
                        sys.stdout.flush()
                        size, blob_name = line.split('\t')
                        tag_frag_size += int(size)
                        total_tree_size += int(size)

            kid_size = _calculate_tag_tree(host, kids, tag_sizes, new_tag_parts)
            tag_frag_size += kid_size
            total_tree_size += kid_size

            if tag_frag not in tag_sizes:
                tag_sizes[tag_prefix] = 0
            tag_sizes[tag_prefix] += tag_frag_size
            # print "PATH: %s" % path
        finally:
            if standard_output:
                os.unlink(standard_output)
    return total_tree_size

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        "-s",
        "--server",
        default='disco://localhost',
        dest="server",
        help="DDFS master")

    parser.add_option(
        "-p",
        "--prefix",
        default='/srv/disco',
        dest="prefix",
        help="Root directory of disco artifacts on clustered nodes.")

    (options, args) = parser.parse_args()

    server = options.server
    if not server.startswith('disco://'):
        server = "disco://%s" % server

    if len(args):
        intag = args[0]
    else:
        intag = ''

    ddfs = DDFS(server)
    tags = ddfs.list(intag)

    host_tags = {}
    print "Fetching blobs "
    for tag in tags:
        # build the host_tags tree

        blobs = ddfs.blobs(tag)
        tag_size = 0
        for blob in blobs:
            print ".",
            sys.stdout.flush()
            replica = blob[random.randint(0, len(blob) - 1)]
            # print "REP: %s %s" % (tag, replica)
            _, (host, port), path = urlsplit(replica)
            path = path.replace('$', '\\$')
            kids, blobs = _find_tag_tree(host_tags, host, tag)
            blobs.append("%s/%s" % (options.prefix, path))

    # print "HTAGS: %s" % repr(host_tags)
    total_size = 0
    tag_sizes = {}
    print
    print "Calculating tree "
    for host, tag_tree in host_tags.iteritems():
        total_size += _calculate_tag_tree(host, tag_tree, tag_sizes, [])

    print
    print '=' * 50
    print "All sizes in MB"
    for tag_frag in sorted(tag_sizes.keys()):
        size = "{:,}".format(tag_sizes[tag_frag]).rjust(14)
        print "%s   %s" % (size, tag_frag)

    print "TOTAL: %s" % total_size


