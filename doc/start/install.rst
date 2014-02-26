.. _installguide:

Installing Hustle
=================

Hustle is hosted on `GitHub <https://github.com/changoinc/hustle>`_ and should be cloned from that repo::

    git clone git@github.com:changoinc/hustle.git

Dependencies
------------

Hustle has the following dependencies:

* you will need `Python 2.7 <http://www.python.org/downloads/>`_
* you will need `Disco 0.5 <http://disco.readthedocs.org/en/latest/start/install.html>`_

Installing the Hustle Client
----------------------------

In order to run Hustle, you will need to install it onto an existing *Disco v0.5* cluster.

In order to query a Hustle/Disco cluster, you will need to install the Hustle software on that *client* machine::

    cd hustle
    sudo ./bootstrap.sh

This will build and install Hustle on your client machine.

Installing on the Cluster
-------------------------

Disco is a distributed system and may have many nodes.  Each of the nodes in your Disco cluster will need to install
the Hustle dependencies.  These can be found in the *hustle/deps* directory.  The easiest way to install Hustle on
your disco slave nodes is to::

    cd hustle/deps
    make
    sudo make install

on **ALL** you disco slave nodes.

You may now want to go and run the :ref:`Integration Tests <integrationtests>` to validate your installation.