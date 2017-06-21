package com.hazelcast.samples.partitioning.web;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;

import static com.hazelcast.samples.partitioning.Styles.*;

/**
 * Created by Alparslan on 7.06.2017.
 */
public class Partition extends VerticalLayout {

    public static final int PRIMARY = 0;
    public static final int BACKUP = 1;
    private int partitionId;
    private final Label partitionNameLabel;
    public static final String PRIMARY_PARTITION_NAME_PREFIX = "Primary #";
    public static final String BACKUP_PARTITION_NAME_PREFIX = "Backup #";
    private String owner;
    private GridLayout entryLayout;
    private final String backgroundColorStyle;

    public Partition(int partitionId, String name, int type) {
        this.partitionId = partitionId;
        partitionNameLabel = new Label(name);
        partitionNameLabel.addStyleName(FONT_BOLD);
        addComponent(partitionNameLabel);
        setComponentAlignment(partitionNameLabel, Alignment.TOP_CENTER);
        setWidth(100, Unit.PIXELS);
        setHeight(130, Unit.PIXELS);
        backgroundColorStyle = getBackgroundColorStyle(type, partitionId);
        addStyleName(backgroundColorStyle);
        addStyleName("partition-margin");
        setMargin(false);
        setSpacing(false);
        addStyleName("partition-vertical-layout-spacing");

        entryLayout = new GridLayout(2, 2);
        entryLayout.setSpacing(true);
        entryLayout.addStyleName("partition-spacing");

//        initMockData();

        addComponent(entryLayout);
        setExpandRatio(entryLayout, 1.0f);
    }

    private String getBackgroundColorStyle(int type, int partitionId) {
//        if (type == PRIMARY) {
//            switch (partitionId) {
//                case 0:
//                    return BACKGROUND_PART0_PRIM;
//                case 1:
//                    return BACKGROUND_PART1_PRIM;
//                case 2:
//                    return BACKGROUND_PART2_PRIM;
//                case 3:
//                    return BACKGROUND_PART3_PRIM;
//                case 4:
//                    return BACKGROUND_PART4_PRIM;
//                case 5:
//                    return BACKGROUND_PART5_PRIM;
//            }
//        }else if (type == BACKUP){
        switch (partitionId) {
            case 0:
                return BACKGROUND_PART0_BACK;
            case 1:
                return BACKGROUND_PART1_BACK;
            case 2:
                return BACKGROUND_PART2_BACK;
            case 3:
                return BACKGROUND_PART3_BACK;
            case 4:
                return BACKGROUND_PART4_BACK;
            case 5:
                return BACKGROUND_PART5_BACK;
        }
//        }
        return null;
    }

    private void initMockData() {
        this.entryLayout.addComponent(new Entry("A"));
        this.entryLayout.addComponent(new Entry("B"));
        this.entryLayout.addComponent(new Entry("C"));
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getOwner() {
        return owner;
    }

    public void addEntry(Entry entry) {
        entryLayout.addComponent(entry);
    }

    public void makeUnvisible() {
        this.addStyleName(BACKGROUND_TRANSPARENT);
        partitionNameLabel.setVisible(false);
        entryLayout.setVisible(false);
    }

    public void makeVisible() {
        this.removeStyleName(BACKGROUND_TRANSPARENT);
        partitionNameLabel.setVisible(true);
        entryLayout.setVisible(true);
    }
}
