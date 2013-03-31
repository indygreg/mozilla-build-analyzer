===========================
Mozilla Build Data Analyzer
===========================

This project provides a mechanism for retrieving, storing, and analyzing
data from automated builds conducted to build Firefox and other related
Mozilla projects.

This project currently focuses mostly on obtaining raw data, extracting
useful information from it, and storing the results in a persistent data
store. Data analysis can then be built off of stored data.

Configuration
=============

Once you've cloned the repository, you'll need to set up your run-time
environment. It starts by setting up a virtualenv::

    $ virtualenv my_virtualenv
    $ source my_virtualenv/bin/activate
    $ python setup.py develop

Next, you'll need to get a Cassandra instance running. If you already have
one running, great. You can connect to that. If not, run the following to
bootstrap a Cassandra instance::

    $ python bin/bootstrap_cassandra.py /path/to/install/root

    # e.g.
    $ python bin/bootstrap_cassandra.py cassandra.local


That program will print out some important info about environment state.
Please note it!

You can copy and paste the output of the bootstrapper to launch
Cassandra. It will look something like::

    $ CASSANDRA_CONF=/home/gps/src/mozilla-build-analyzer/cassandra.local/conf cassandra.local/apache-cassandra-1.2.3/bin/cassandra -f

If you don't want to run Cassandra in the foreground, just leave off
the *-f*.

Running
=======

Now that you have your environment configured and Cassandra running, you'll
want to populate some data.

The main interface to all actions is **mbd.py**. It's just a gateway script
(like mach) that invokes sub commands.

The first thing you'll want to do is obtain information about builds and
jobs that have executed. This data is stored in per-day archives on
Mozilla servers, so we import by day::

    # Import all job data for March 28, 2013.
    $ python bin/mbd.py load-job-data 2013-03-28

Next, we can obtain the raw logs for each job. This allows us to perform
deep analysis based on log content::

    # Automatically import missing logs.
    $ python bin/mbd.py load-raw-logs

Importing logs takes a long time. And, it consumes a *lot* of internet
bandwidth. But, the good news is you only need to do this once (at least
once per job).

Currently, that's all you can do. Useful features will come soon.

