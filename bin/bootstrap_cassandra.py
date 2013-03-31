# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import shutil
import sys
import tarfile
import urllib2

from StringIO import StringIO


VERSION = '1.2.3'
DOWNLOAD_URL = 'http://mirrors.sonic.net/apache/cassandra/1.2.3/apache-cassandra-1.2.3-bin.tar.gz'

ourdir = os.path.dirname(__file__)
topdir = os.path.normpath(os.path.join(ourdir, '..'))

def ensure_conf_files(template_path, install_path, max_heap='1G',
        heap_newsize='500M'):
    conf_path = os.path.join(install_path, 'conf')

    if os.path.exists(conf_path):
        print('Configuration files already present. Not doing anything.')
        return

    # Else we copy the default Cassandra config files then install our own
    # overrides.
    vendor_conf_path = os.path.join(install_path, 'apache-cassandra-%s' % VERSION,
        'conf')

    shutil.copytree(vendor_conf_path, conf_path)

    # Now install our overrides.
    data_path = os.path.join(install_path, 'data')
    log_path = os.path.join(install_path, 'log')

    for filename in os.listdir(template_path):
        if not filename.endswith('.in'):
            continue

        input_path = os.path.join(template_path, filename)
        template = open(input_path).read()

        print('Installing custom config file: %s' % filename[0:-3])
        with open(os.path.join(conf_path, filename[0:-3]), 'w') as fh:
            fh.write(template.format(dataroot=data_path, logroot=log_path))

    # Set reasonable default JVM memory size.
    default_env_path = os.path.join(vendor_conf_path, 'cassandra-env.sh')
    our_env_path = os.path.join(conf_path, 'cassandra-env.sh')

    print('Adjusting JVM heap size in cassandra-env.sh')
    with open(default_env_path) as default, open(our_env_path, 'w') as ours:
        for line in default:
            if line.startswith('#MAX_HEAP_SIZE='):
                ours.write('MAX_HEAP_SIZE=%s\n' % max_heap)
            elif line.startswith('#HEAP_NEWSIZE='):
                ours.write('HEAP_NEWSIZE=%s\n' % heap_newsize)
            else:
                ours.write(line)


def main(install_path):
    install_path = os.path.abspath(install_path)
    print('Installing Cassandra into %s' % install_path)

    cassandra_path = os.path.join(install_path, 'apache-cassandra-%s' % VERSION)

    # If the binary package isn't present, download and uncompress it.
    if not os.path.exists(cassandra_path):
        print('Downloading Cassandra from %s' % DOWNLOAD_URL)
        url = urllib2.urlopen(DOWNLOAD_URL)
        data = StringIO(url.read())

        print('Uncompressing to %s' % cassandra_path)
        t = tarfile.open(fileobj=data)
        t.extractall(install_path)

        if not os.path.isdir(cassandra_path):
            print('Could not find extracted files. Bootstrapper error.')
            return 1

    # Create configuration.
    conf_path = os.path.join(install_path, 'conf')
    ensure_conf_files(os.path.join(topdir, 'conf'), install_path)

    # Create directories.
    data_path = os.path.join(install_path, 'data')
    log_path = os.path.join(install_path, 'log')

    for p in [data_path, log_path]:
        if not os.path.exists(p):
            os.mkdir(p)

    print('Cassandra is ready to go!')
    print('You will need to set CASSANDRA_CONF in your environment for things')
    print('to work properly. e.g.')
    print('')
    print('  export CASSANDRA_CONF=%s' % conf_path)
    print('')
    print('To run Cassandra, do something like the following:')
    print('  CASSANDRA_CONF=%s %s/bin/cassandra -f' % (conf_path,
        cassandra_path))
    print('')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: bootstrap_cassandra.py path/to/install/location')
        sys.exit(1)

    sys.exit(main(sys.argv[1]))
