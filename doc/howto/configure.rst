.. _configureguide:

Configuring Hustle
==================

Hustle has a configuration file located at::

    /etc/hustle/settings.yaml

and has the following possible settings:

==============      ==============================      ==============================================
Name                Default Value                       Description
==============      ==============================      ==============================================
server              disco://localhost                   The Disco master node
worker_class        hustle.core.pipeworker.Worker       The Disco Worker class
dump                False                               True will automatically print select() results
nest                False                               True will return a Table from select()
partition           16                                  The number of partitions for restrict-select
history_size        1000                                The number of history entries in the CLI
==============      ==============================      ==============================================

