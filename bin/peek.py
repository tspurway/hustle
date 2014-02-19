from hustle import Table

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        "-c",
        "--columns",
        default=None,
        dest="columns",
        help="the columns to display (comman separated)")

    parser.add_option(
        "-n",
        "--number-of-rows",
        default=10,
        dest="number_of_rows",
        type=int,
        help="the number of rows of each column to display 0 for all")

    (options, args) = parser.parse_args()


    tab = Table.from_file(args[0])
    env, txn, dbs, meta = tab._open(args[0])

    number_of_rows = options.number_of_rows
    columns = None
    if options.columns:
        columns = set(options.columns.split(','))

    for attr, value in meta.dup_items(txn):
        print "%s -> %s" % (attr, value)

    for name, (db, typ, subdb) in dbs.items():
        if not columns or name in columns:
            print "=" * 132
            print "Name: %s (%s) (%s)" % (name, type(db), typ)
            i = 0
            for rid, val in db.dup_items(txn):
                print "%s, %s" % (rid, val)
                if i >= number_of_rows:
                    break
                i += 1

            if subdb:
                print "INDEX: "
                i = 0
                for rid, val in subdb.dup_items(txn):
                    print "    %s, %s" % (rid, val)
                    if i >= 10:
                        break
                    i += 1