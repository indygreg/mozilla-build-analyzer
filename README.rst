===========================
Mozilla Build Data Analyzer
===========================

This project provides a mechanism for retrieving, storing, and analyzing
data from automated builds conducted to build Firefox and other related
Mozilla projects.

The bulk of this project is a data store that is effectively a shadow-copy
of build data that is canonically stored on Mozilla's servers. We use
Cassandra as the storage backend. Only some analysis is provided. However,
additional analysis can be facilitated by combing through the mountain of
data collected that you now have easy access to.

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

Build metadata is the most important data type. It defines what things are
built and details and how and when they are built. Build metadata consists
of the following data types:

builders
    Describes a specific build configuration. e.g. *xpcshell tests on
    mozilla-central on Windows 7 for opt builds* is a builder.

builds
    These are a specific invocation of a builder. These are arguably the
    most important data type. Most of our data is stored against a
    specific build instance.

slaves
    These are the machines that perform build jobs. There are over 1000
    of them in Mozilla's network.

masters
    These coordinate what the slaves do. You don't need to be too concerned
    with these.

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
only what you need. Here are some ideas for intelligently importing logs::

    # Only import logs for mozilla-central.
    $ mbd build-logs-synchronize --category mozilla-central

    # Only import xpcshell logs for PGO builds.
    $ mbd build-logs-synchronize --builder-pattern '*pgo*xpchsell*'

    # Windows 7 reftests.
    $ mbd build-logs-synchronize --builder-pattern mozilla-central_win7_test_pgo-reftest

    # Only import logs for mozilla-central after 2013-03-28.
    $ mbd build-logs-synchronize --after 2013-03-28 --category mozilla-central

Importing logs takes a long time. And, it consumes a *lot* of bandwidth.
But, the good news is you only need to do this once (at least once per
build) because logs are idempotent.

Analyzing Data
==============

Run mbd with --help for a list of all the commands. Here are some::

    # Print the names of all slaves.
    $ mbd slave-names

    # Print builds performed on a specific slave.
    $ mbd slave-builds bld-linux64-ec2-413

    # Print a table listing total times slaves were running builds.
    $ mbd slave-efficiencies

    # Print all the builders associated with a builder category.
    $ mbd builders-in-category --print-name mozilla-central

    # Print names of all known builders.
    $ mbd builder-names

    # Print build ID that occurred on a builder.
    $ mbd builds-for-builder mozilla-central_ubuntu32_vm_test-xpcshell

    # Print the raw log output for a build.
    $ mbd log-cat 21177014

    # View times for all mozilla-central builders.
    $ mbd build-times --category mozilla-central

You can even perform some advanced pipeline tricks, such as printing all the
logs for a single builder::

    $ mbd builds-for-builder mozilla-central_ubuntu32_vm_test-xpcshell | xargs mbd log-cat

Disclaimer
==========

The current state of this project is very alpha. Schemas will likely change.
There are no guarantees that time spent importing data will not be lost. But
if you have a faster internet connection and don't mind the inconvenience, go
right ahead.

Planned Features
================

This project is still in its infancy. There are many planned features.

One of the biggest areas for future features is more log parsing. One of the
original goals was to facilitate extraction of per-test metadata from things
like xpcshell test logs, for example.

We may also consider collecting additional files from public servers. e.g.
there's no reason we can't store the binary archives and perform symbol
analysis, etc.

Frequently Asked Questions
==========================

Why?
----

The original author (Gregory Szorc) frequently wanted to perform analysis
over large sets of build data. Fetching logs individually was often slow
and had high latency. He didn't want to deal with this so he instead
created a system for interacting with an offline shadow copy. The results
are what you see.

Why Cassandra?
--------------

While SQL would have been a fine choice, the author didn't want to deal
with writing SQL. He also had previous experience with Cassandra from
before it hit 1.0. He was not only interested in seeing what all has
changed, but he was also looking for something familiar he could easily
implement. Even if the author didn't have experience with Cassandra, he
would still consider Cassandra because of its operational characteristics.

Is this an official Mozilla project?
------------------------------------

Not at this time. Although, it's very similar to Datazilla and TBPL, so
it's possible it may evolve into one.

By copying everything you are creating high load on Mozilla's FTP servers
-------------------------------------------------------------------------

Yup. But if you perform analysis on all of this data, the net outcome
is good for the central servers because you don't touch them after
the initial data fetch.

