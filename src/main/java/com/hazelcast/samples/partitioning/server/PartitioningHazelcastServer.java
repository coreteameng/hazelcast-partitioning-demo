package com.hazelcast.samples.partitioning.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.samples.partitioning.HazelcastService;
import com.hazelcast.samples.partitioning.event.HazelcastPartitioningEvent;
import com.hazelcast.samples.partitioning.event.MemberAddedEvent;
import com.hazelcast.samples.partitioning.event.MemberRemovedEvent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Alparslan on 11.06.2017.
 */
public class PartitioningHazelcastServer {

    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
    private static HazelcastInstance hazelcastInstance;

    public static void main(String[] args) {
        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "6");
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> initial = hazelcastInstance.getMap("initial");
        initial.put("key", "value");
        hazelcastInstance.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                processMembershipEvent(membershipEvent);
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                processMembershipEvent(membershipEvent);
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

            }
        });
    }

    private static void processMembershipEvent(MembershipEvent membershipEvent) {
        executor.execute(() -> {
            waitForMigrationsToBeCompleted();
            IQueue<HazelcastPartitioningEvent> queue = hazelcastInstance.getQueue(HazelcastService.EVENT_QUEUE_NAME);
            try {
                HazelcastPartitioningEvent event;
                if (membershipEvent.getEventType() == MembershipEvent.MEMBER_ADDED) {
                    event = new MemberAddedEvent(membershipEvent.getMember().getAddress().toString(),
                            membershipEvent.toString());
                } else if (membershipEvent.getEventType() == MembershipEvent.MEMBER_REMOVED) {
                    event = new MemberRemovedEvent(membershipEvent.getMember().getAddress().toString(),
                            membershipEvent.toString());
                } else {
                    throw new Exception("Not implemented");
                }
                queue.put(event);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void waitForMigrationsToBeCompleted() {
        while (!hazelcastInstance.getPartitionService().isClusterSafe()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
