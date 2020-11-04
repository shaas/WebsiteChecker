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

Run:

.. code-block:: bash

    python3 websiteCheck.py # Runs the Kafka Producer and checks the websites
    python3 websiteConsumer.py # Runs the Kafka Consumer and submits entries to the DB

Test:

.. code-block:: bash

    make test