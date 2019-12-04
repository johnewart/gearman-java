Gearman Java Implementation
===========================

[![Build
Status](https://travis-ci.org/johnewart/gearman-java.svg)](https://travis-ci.org/johnewart/gearman-java)

An implementation of the [gearman protocol](http://www.gearman.org) server, client, and worker in Java. Features include:

* Pluggable persistent storage mechanism currently supporting:
    * PostgreSQL
    * Redis
    * Memory-only
* Web-based UI dashboard
* Metrics using [java metrics](https://github.com/codahale/metrics)
* Multi-threaded server using Netty for high-performance network I/O
* High-performance - on a single m3.2xlarge EC2 instance with 8 on-box Ruby
  clients it has achieved over 11,000 jobs per second with in-memory
  storage


Getting Started
---------------

Quick start:

1. Download the [latest pre-built SNAPSHOT release](https://oss.sonatype.org/content/repositories/snapshots/net/johnewart/gearman/gearman-server/) from the Sonatype snapshots repository
2. Run `java -jar gearman-server-VERSION.jar`
3. This will default to port 4730 and memory-only persistence, with snapshotting and the web interface listening on port 8080
4. To list avaliable options run `java -jar gearman-server-VERSION.jar --help`

If you want to use more advanced features, run server with config file `java -jar gearman-server-VERSION.jar -c path_to_config_file`.
For details check example configuration files in [the gearman-server sub-project](https://github.com/johnewart/gearman-java/tree/master/gearman-server)


Web Interface
-------------

Some of the issues that I've run into in the past have been related to visibility into job queues. To address this, I've added a web management console that lets you see the state of the system. For small installations this is a nice option because it doesn't require you to setup or have any external monitoring systems. Some screenshots here:

### Main Dashboard

![Web dashboard](https://github.com/johnewart/gearman-java/raw/master/misc/dashboard.jpg)

### All Queues

![All Queues](https://github.com/johnewart/gearman-java/raw/master/misc/queues.jpg)

### Per-Queue Status

![Queue overview](https://github.com/johnewart/gearman-java/raw/master/misc/queue.jpg)


Contributing
------------

Feel free to fork and submit pull requests, or test and submit bug reports.

Author
-------

John Ewart [http://johnewart.net](http://johnewart.net)

Contributors
------------

Some tiny portions of this project leverage code from the java-gearman-service project.
