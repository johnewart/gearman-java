Gearman Java Server
===================

An implementation of the [gearman protocol](http://www.gearman.org) server (and basic client) in Java. Features include:

* Pluggable persistent storage mechanism currently supporting:
    * PostgreSQL
    * Redis
    * Memory-only
* Web-based UI dashboard
* Metrics using [java metrics](https://github.com/codahale/metrics)
* Multi-threaded server using Netty for high-performance network I/O


Getting Started
---------------

Quick start:

1. Download the [latest version pre-built](http://code.johnewart.net/maven/org/gearman/gearman-server/0.6 .0/gearman-server-0.6.0.jar) from my Maven repository 
2. Run java -jar gearman-server-0.6.0.jar 
3. This will default to port 4730 and memory-only persistence, with snapshotting and the web interface listening on port 8080

Command-line options:

You can see what command-line options are available by passing -h or --help on the command line. Currently supported:

<pre>
<code>
usage: java -jar gearman-server.jar [options]
    --disable-monitor         Disable job queue monitor
 -h,--help                    Display this message
    --port <arg>              Port to listen on
    --postgres-dbname <arg>   PostgreSQL database name
    --postgres-host <arg>     PostgreSQL hostname
    --postgres-pass <arg>     PostgreSQL password
    --postgres-port <arg>     PostgreSQL port
    --postgres-user <arg>     PostgreSQL user
    --redis-host <arg>        Redis hostname
    --redis-port <arg>        Redis port
    --storage <arg>           Storage engine to use (redis, postgresql),
                              default is memory only
</code>
</pre>

Web Interface
-------------

Some of the issues that I've run into in the past have been related to visibility into job queues. To address this, I've added a web management console that lets you see the state of the system. For small installations this is a nice option because it doesn't require you to setup or have any external monitoring systems. Some screenshots here:

![Web dashboard](https://github.com/johnewart/gearman-java/raw/master/misc/dashboard.jpg)

![Queue overview](https://github.com/johnewart/gearman-java/raw/master/misc/queue.jpg)


Contributing
------------

Feel free to fork and submit pull requests, or test and submit bug reports.

Author
-------

John Ewart [@soysamurai](https://twitter.com/soysamurai), [http://johnewart.net](http://johnewart.net)

Contributors
------------

Some small portions of this leverage code from the java-gearman-service project, as this started because I wanted to add persistence to that service but decided to write it from (mostly) scratch. 