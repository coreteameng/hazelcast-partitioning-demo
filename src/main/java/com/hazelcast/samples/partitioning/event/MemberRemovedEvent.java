package com.hazelcast.samples.partitioning.event;

/**
 * Created by Alparslan on 12.06.2017.
 */
public class MemberRemovedEvent implements HazelcastPartitioningEvent {

    private String memberAddress;
    private String eventString;

    public MemberRemovedEvent(String memberAddress, String eventString) {
        this.memberAddress = memberAddress;
        this.eventString = eventString;
    }

    public String getMemberAddress() {
        return memberAddress;
    }

    public void setMemberAddress(String memberAddress) {
        this.memberAddress = memberAddress;
    }

    public String getEventString() {
        return eventString;
    }

    public void setEventString(String eventString) {
        this.eventString = eventString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberRemovedEvent that = (MemberRemovedEvent) o;

        return eventString.equals(that.eventString);
    }

    @Override
    public int hashCode() {
        return eventString.hashCode();
    }
}
