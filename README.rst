Website Checker
===============

This simple project is an example of how to use Kafka Consumers and Producers.


Quickstart
----------

Install requirements:

.. code-block:: bash

    sudo zypper in python-pip
    make init

Configure:

.. code-block:: bash

    cp configuration.env .env
    vim .env # Make any changes needed

Test:

.. code-block:: bash

    make test

Examples
--------

* ``websiteCheck.py`` is an example how to use the ``Website``-class to meassure websites and how to publish the results via the ``WCKafka``-module to an existing Kafka instance.

* ``websiteConsumer.py`` is an example how to use the ``Database``-class to add results to a PostgreSQL database. It also shows how to check and obtain new messages from an existing Kafka instance via the ``WCKafka``-module.

.. code-block:: bash

    python3 websiteCheck.py # Runs the Kafka Producer and checks the websites
    python3 websiteConsumer.py # Runs the Kafka Consumer and submits entries to the DB