===========================
Mozilla Build Data Analyzer
===========================

This project provides a mechanism for retrieving, storing, and analyzing
data from automated builds conducted to build Firefox and other related
Mozilla projects.

The bulk of this project is a data store that is effectively a shadow-copy
of build data which is canonically stored on Mozilla's servers. We use
Cassandra as the storage backend. Only some analysis is provided. However,
additional analysis can be facilitated by combing through the mountain of
data collected.

You can think of this project as a combination of the TBPL database,
Mozilla's FTP server (which hosts the output of all jobs), and Datazilla
(a database used to store Talos and other build job results).

Initial Setup
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

**At this time, it is highly recommended to use the local Cassandra instance
instead of connecting to a production cluster.**

Workflow
========

Now that you have your environment configured and Cassandra running, you'll
want to populate some data.

The main interface to all actions is **mbd**. It's just a gateway script
that invokes sub commands (like *mach* if you are a Firefox developer).

Populating Data
===============

You need to explicitly tell your deployment which Mozilla build data to
import. The sections below detail the different types of build data
that can be loaded.

Build Metadata
--------------

Build metadata is the most important data type. It defines the set of
known job types (builders) that get run by Mozilla, the slaves
(machines) they run on, and details for each invocation of those (jobs).

Build metadata is canonically defined by a bunch of JSON files sitting
on a public HTTP server. The first step to loading build metadata is to
synchronize these files with a local copy::

    $ mbd build-files-synchronize

This will take a while to run initially because there are cumulatively many
gigabytes of data. If you don't feel like waiting around for all of them to
download, just ctrl-c after you've fetched enough!

Now that we have a copy of the raw build data, we need to extract the
useful parts and load them into our local store.

Here is how we load the last week of data::

    $ mbd build-metadata-load --day-count 7

At this point, you can conduct analysis of build metadata!

Remember, as time goes by, you'll need to continually refresh the build
files and re-load build metadata!

Build Logs
----------

Build metadata can be supplemented with data parsed from the logs of
individual jobs. Loading log data follows the same mechanism as build
metadata.

Because logs are quite large (tens of gigabytes) and since you are likely
only interested in a subset of logs, it's usually a good idea to import
only what you need. Here is how you would import just xpcshell test logs::

    $ mbd load-raw-logs --builder-pattern '*xpcshell*'

Importing logs takes a long time. And, it consumes a *lot* of internet
bandwidth. But, the good news is you only need to do this once (at least
once per job) because logs are idempotent.

*Currently we don't do anything with logs, so it's probably not worth it to
fetch them yet.*

Analyzing Data
==============

Run mbd with --help for a list of all the commands. Here are some::

    # Print the names of slaves.
    $ mbd slave-names

    # Print jobs performed on a specific slave.
    $ mbd slave-jobs bld-linux64-ec2-413

    # Print a table listing total times slaves were running jobs.
    # slave-efficiencies

