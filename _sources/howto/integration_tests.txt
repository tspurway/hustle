.. _integrationtests:

Hustle Integration Test Suite
=============================

The Hustle Integration Test suite is a good place  to see non-trivial Hustle Tables created,
data inserted into them, and some subsequent queries.  They are located in::

    hustle/integration_test

To run the test suite, ensure you have installed `Nose <https://nose.readthedocs.org/en/latest/>`_ and
:ref:`Hustle <installguide>`.  Before you run the integration tests, you will need to make sure
`Disco <http://discoproject.org/>`_ is running and that you have run the *setup.py* script once::

    python hustle/integration_test/setup.py

You can then execute the *nosetests* in the integration suite::

    cd hustle/integration_test
    nosetests

