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

Doing Stuff
===========

Now that you have your environment configured and Cassandra running, you'll
want to populate some data.

The main interface to all actions is **mbd**. It's just a gateway script
that invokes sub commands (like *mach* if you are a Firefox developer).

Populating Data
===============

In order to do anything useful, you'll need to populate data. While there
should be a single command to ensure all data is up to date, that doesn't
exist yet. Until then, you'll have to use low-level commands.

The first thing you'll want to do is obtain the raw build metadata files
from Mozilla. These files contain details about all the jobs that ran.

This data is always changing, so we offer a single command to synchronize
the files with our local storage::

    $ mbd build-files-synchronize

This will take a while to run initially because there are cumulatively many
gigabytes of raw build data. If you don't feel like waiting around for all
of them to download, just ctrl-c after you've fetched enough for your like!

Now that we have the raw data, we need extract metadata and import it into
storage. Because this can take a while and because you may only be interested
in certain days, you get to control which days to import::

    $ mbd build-files-load --day 2013-03-27

At this point, you have enough information in the system to perform data
analysis! You can stop here or continue to import job logs.

The above has simply imported basic job metadata. This only tells a partial
story. More information is available from the raw logs of each job. Because
logs are quite large and you probably don't need all of them, you must
manually import job logs.

It's usually a good idea to limit job importing to jobs you care about. e.g.
say you want to analyze xpcshell test logs::

    $ python bin/mbd.py load-raw-logs --builder-pattern '*xpcshell*'

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

