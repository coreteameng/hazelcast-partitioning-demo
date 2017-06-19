package com.hazelcast.samples.partitioning.entryprocessor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.Address;

import java.util.Map;

/**
 * Created by Alparslan on 11.06.2017.
 */
public class PartitionTableAccessor implements EntryProcessor, HazelcastInstanceAware {
    private transient HazelcastInstance hazelcastInstance;

    public Object process(Map.Entry entry) {
        HazelcastInstanceImpl instanceImpl = (HazelcastInstanceImpl) hazelcastInstance;
        Node node = instanceImpl.node;
        InternalPartitionService partitionService = node.getPartitionService();
        PartitionTableView partitionTableView = partitionService.createPartitionTableView();
        int partitionCount = partitionService.getPartitionCount();
        String[][] addresses = new String[partitionCount][7];
        for (int i = 0; i < partitionCount; i++) {
            Address[] partitionTableViewAddresses = partitionTableView.getAddresses(i);
            for (int j = 0; j < partitionTableViewAddresses.length; j++) {
                Address partitionTableViewAddress = partitionTableViewAddresses[j];
                if (partitionTableViewAddress != null) {
                    addresses[i][j] = partitionTableViewAddress.toString();
                }
            }
        }

        return addresses;
    }

    public EntryBackupProcessor getBackupProcessor() {
        return null;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
