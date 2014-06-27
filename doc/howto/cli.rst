.. _cliguide:

Hustle Command Line Interface (CLI)
===================================

After installing Hustle, you can invoke the Hustle CLI from the installation directory like this::

    bin/hustle

Assuming you've installed everything and have a running and correctly configured *Disco* instance, you will get a
Python prompt looking something like this::

    ➜  bin git:(develop) ✗ ./hustle
    Loading Hustle Tables from disco://localhost
       impressions
       pixels
    Welcome to Hustle!  Type `commands()` or `tables()` for some help, `exit()` to leave.
    >>>

We see here that the CLI has loaded the Hustle tables from the *disco://localhost* cluster called *impressions*
and *pixels*.  The CLI actually loads these into Python's global variable space, so that these
:class:`Tables <hustle.Table>` are actually instantiated with their table names in the Python namespace::

    >>> schema(impressions)
    ad_id (int32,IX)                  cpm_millis (uint32)               date (string,IX,PT)
    site_id (dict(32),IX)             time (uint32,IX)                  token (string,IX)
    url (dict(32))

gives the *schema* of the *impressions* table.  Doing a query is just as simple::

    >>> select(impressions.ad_id, h_sum(impressions.cpm_millis), where=impressions.date == '2014-01-20')
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                                       ad_id                          sum(cpm_millis)
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                                      30,016                                    1,690
                                      30,003                                      925
                                      30,019                                    2,023
                                      30,024                                    1,511
                                      30,009                                      863
                                      30,025                                    3,124
                                      30,010                                    2,555
                                      30,011                                    2,150
                                      30,014                                    4,491


The CLI offers the following features over and above being a 'normal' Python REPL:
- configurable command history
- no *import* statements required to load Hustle functionality
- auto-completion (with TAB key) of all Hustle functions, Tables, and Columns
- query results (from :func:`select <hustle.select>` are automatically sent to *stdout*
