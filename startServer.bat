@ECHO OFF

mvn exec:java "-Dexec.mainClass=com.hazelcast.samples.partitioning.server.PartitioningHazelcastServer"