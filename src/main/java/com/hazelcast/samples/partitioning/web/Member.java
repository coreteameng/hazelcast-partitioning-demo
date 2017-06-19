package com.hazelcast.samples.partitioning.web;

import com.vaadin.icons.VaadinIcons;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.ui.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.samples.partitioning.Styles.*;

/**
 * Created by Alparslan on 7.06.2017.
 */
public class Member extends VerticalLayout {

    public static final String MEMBER_NAME_PREFIX = "Member #";
    private final Label memberNameLabel = new Label(MEMBER_NAME_PREFIX + "1 ([127.0.0.1]:5701)");
    private final CssLayout primaryPartitionsLayout;
    private final CssLayout backupPartitionsLayout;
    private String memberAddress;
    private Map<Integer, Partition> primaryPartitionMap = new ConcurrentHashMap<>();
    private Map<Integer, Partition> backupPartitionMap = new ConcurrentHashMap<>();

    public Member(String memberAddress) {
        this.memberAddress = memberAddress;
        setDefaultComponentAlignment(Alignment.MIDDLE_CENTER);
        addStyleName(MEMBER_BACKGROUND);
        setHeight(100, Unit.PERCENTAGE);
        memberNameLabel.addStyleName(COLOR_WHITE);
        addComponent(memberNameLabel);
        setMargin(false);
        addStyleName("member-padding");
        HorizontalLayout allPartitionsLayout = new HorizontalLayout();
        allPartitionsLayout.setWidth(100, Unit.PERCENTAGE);
        allPartitionsLayout.setDefaultComponentAlignment(Alignment.MIDDLE_CENTER);
        addComponent(allPartitionsLayout);

        primaryPartitionsLayout = new CssLayout();
//        primaryPartitionsLayout.setWidth(340, Unit.PIXELS);
        primaryPartitionsLayout.setWidth(100, Unit.PERCENTAGE);
        primaryPartitionsLayout.addStyleName("member-internal-spacing");
        primaryPartitionsLayout.addStyleName(PARTITIONS_BACKUP_BORDER);

        Label seperator = new Label();
        seperator.setContentMode(ContentMode.HTML);
        StringBuilder html = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            html.append(VaadinIcons.LINE_V.getHtml());
            html.append("<br>");
        }
        seperator.setValue(html.toString());

        backupPartitionsLayout = new CssLayout();
//        backupPartitionsLayout.setWidth(340, Unit.PIXELS);
        backupPartitionsLayout.setWidth(100, Unit.PERCENTAGE);
        backupPartitionsLayout.addStyleName("member-internal-spacing");
        backupPartitionsLayout.addStyleName(PARTITIONS_BACKUP_BORDER);

//        initMockData();

        allPartitionsLayout.addComponents(primaryPartitionsLayout, seperator, backupPartitionsLayout);
        allPartitionsLayout.setExpandRatio(primaryPartitionsLayout, 0.9f);
        allPartitionsLayout.setExpandRatio(seperator, 0.05f);
        allPartitionsLayout.setExpandRatio(backupPartitionsLayout, 0.9f);

    }

    private void initMockData() {
        primaryPartitionsLayout.addComponent(new Partition(0, "Partition #1", Partition.PRIMARY));
        primaryPartitionsLayout.addComponent(new Partition(1, "Partition #2", Partition.PRIMARY));
        primaryPartitionsLayout.addComponent(new Partition(2, "Partition #3", Partition.PRIMARY));
        primaryPartitionsLayout.addComponent(new Partition(3, "Partition #4", Partition.PRIMARY));
        primaryPartitionsLayout.addComponent(new Partition(4, "Partition #5", Partition.PRIMARY));
        primaryPartitionsLayout.addComponent(new Partition(5, "Partition #6", Partition.PRIMARY));
        backupPartitionsLayout.addComponent(new Partition(0, "Partition #1", Partition.PRIMARY));
        backupPartitionsLayout.addComponent(new Partition(1, "Partition #2", Partition.PRIMARY));
        backupPartitionsLayout.addComponent(new Partition(2, "Partition #3", Partition.PRIMARY));
        backupPartitionsLayout.addComponent(new Partition(3, "Partition #4", Partition.PRIMARY));
        backupPartitionsLayout.addComponent(new Partition(4, "Partition #5", Partition.PRIMARY));
        backupPartitionsLayout.addComponent(new Partition(5, "Partition #6", Partition.PRIMARY));
    }

    public void setMemberName(String memberName) {
        memberNameLabel.setValue(memberName);
    }

    public void addPrimaryPartition(Partition partition) {
        if (!primaryPartitionMap.containsKey(partition.getPartitionId())) {
            primaryPartitionMap.put(partition.getPartitionId(), partition);
            primaryPartitionsLayout.addComponent(partition);
            partition.setOwner(memberAddress);
        }
    }

    public void removePrimaryPartition(Partition partition) {
        if (primaryPartitionMap.containsKey(partition.getPartitionId())) {
            primaryPartitionMap.remove(partition.getPartitionId());
            primaryPartitionsLayout.removeComponent(partition);
            partition.setOwner(null);
        }
    }

    public void addBackupPartition(Partition partition) {
        if (!backupPartitionMap.containsKey(partition.getPartitionId())) {
            backupPartitionMap.put(partition.getPartitionId(), partition);
            backupPartitionsLayout.addComponent(partition);
            partition.setOwner(memberAddress);
        }
    }

    public void removeBackupPartition(Partition partition) {
        if (backupPartitionMap.containsKey(partition.getPartitionId())) {
            backupPartitionMap.remove(partition.getPartitionId());
            backupPartitionsLayout.removeComponent(partition);
            partition.setOwner(null);
        }
    }

    public String getMemberAddress() {
        return memberAddress;
    }

    public void setMemberAddress(String memberAddress) {
        this.memberAddress = memberAddress;
    }

    public boolean containsInPrimaries(int partitionId) {
        return primaryPartitionMap.containsKey(partitionId);
    }

    public boolean containsInBackups(int partitionId) {
        return backupPartitionMap.containsKey(partitionId);
    }
}
