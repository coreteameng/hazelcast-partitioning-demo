Hazelcast Partitioning Demo Application
=======================================

This application is for demonstrating Hazelcast partitioning process details.


How to run
==========

There are two applications: Web app and Hazelcast Server

Running Web app
---------------

To run web app; run `mvn jetty:run` command on the command line.

Running Hazelcast Server
------------------------

To run Hazelcast server; run `mvn exec:java "-Dexec.mainClass=com.hazelcast.samples.partitioning.server.PartitioningHazelcastServer"`
 or run the com.hazelcast.samples.partitioning.server.PartitioningHazelcastServer class from your IDE.