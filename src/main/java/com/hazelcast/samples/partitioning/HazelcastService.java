package com.hazelcast.samples.partitioning;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.samples.partitioning.entryprocessor.PartitionTableAccessor;
import com.hazelcast.samples.partitioning.event.HazelcastPartitioningEvent;
import com.hazelcast.samples.partitioning.web.Member;
import com.hazelcast.samples.partitioning.web.Partition;

import java.util.*;

/**
 * Created by Alparslan on 11.06.2017.
 */
public class HazelcastService {
    private static final HazelcastService instance = new HazelcastService();
    private final HazelcastInstance hzClient;
    public static final String EVENT_QUEUE_NAME = "EVENT_QUEUE_NAME";
    private String mapName = "DEMO";
    private Map<String, List<Integer>> members = new HashMap<>();

    public HazelcastService() {
        hzClient = HazelcastClient.newHazelcastClient();
    }

    public static HazelcastService getInstance() {
        return instance;
    }

    public Collection<Member> getExistingMembers(Map<Integer, Partition> primaryPartitionMap,
                                                 Map<Integer, Partition> backupPartitionMap) {
        ArrayList<Member> members = new ArrayList<>();
        String[][] partitions = getPartitionTable();
        Set<com.hazelcast.core.Member> hzMembersChanged = hzClient.getCluster().getMembers();
        int count = 0;
        for (com.hazelcast.core.Member hzMember : hzMembersChanged) {
            String memberAddress = hzMember.getAddress().toString();
            Member memberUI = new Member(memberAddress);
            memberUI.setMemberName(Member.MEMBER_NAME_PREFIX + count++ + " " + memberAddress);
            List<Integer> primaryPartitionsForMember = getPartitionsForMember(memberAddress, partitions, 0);
            for (Integer partitionId : primaryPartitionsForMember) {
                memberUI.addPrimaryPartition(primaryPartitionMap.get(partitionId));
            }
            List<Integer> backupPartitionsForMember = getPartitionsForMember(memberAddress, partitions, 1);
            for (Integer partitionId : backupPartitionsForMember) {
                memberUI.addBackupPartition(backupPartitionMap.get(partitionId));
            }
            members.add(memberUI);
        }
        return members;
    }

    public String[][] getPartitionTable() {
        return (String[][]) hzClient.getMap(mapName)
                .executeOnKey(1, new PartitionTableAccessor());
    }

    private List<Integer> getPartitionsForMember(String memberAddress, String[][] partitions, int replicaIndex) {
        ArrayList<Integer> memberPartitions = new ArrayList<>();
        for (int i = 0; i < partitions.length; i++) {
            String[] partition = partitions[i];
            if (partition[replicaIndex] != null && partition[replicaIndex].equals(memberAddress)) {
                memberPartitions.add(i);
            }
        }
        return memberPartitions;
    }

    public static void main(String[] args) {
        HazelcastService.getInstance().getExistingMembers(null, null);
    }

    public int getPartitionCount() {
        return hzClient.getPartitionService().getPartitions().size();
    }

    public IQueue<HazelcastPartitioningEvent> getEventQueue() {
        return hzClient.getQueue(EVENT_QUEUE_NAME);
    }

    public int getPartitionId(String key) {
        return hzClient.getPartitionService().getPartition(key).getPartitionId();
    }

    public void put(String key, String value) {
        hzClient.getMap(mapName).put(key, value);
    }

    public String get(String key) {
        return (String) hzClient.getMap(mapName).get(key);
    }
}
