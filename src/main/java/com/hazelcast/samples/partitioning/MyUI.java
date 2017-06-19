package com.hazelcast.samples.partitioning;

import com.hazelcast.core.IQueue;
import com.hazelcast.samples.partitioning.event.HazelcastPartitioningEvent;
import com.hazelcast.samples.partitioning.event.MemberAddedEvent;
import com.hazelcast.samples.partitioning.event.MemberRemovedEvent;
import com.hazelcast.samples.partitioning.web.Entry;
import com.hazelcast.samples.partitioning.web.Member;
import com.hazelcast.samples.partitioning.web.Partition;
import com.vaadin.annotations.Push;
import com.vaadin.annotations.Theme;
import com.vaadin.annotations.VaadinServletConfiguration;
import com.vaadin.icons.VaadinIcons;
import com.vaadin.server.FileResource;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinService;
import com.vaadin.server.VaadinServlet;
import com.vaadin.shared.communication.PushMode;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.ui.*;

import javax.servlet.annotation.WebServlet;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.samples.partitioning.Styles.*;

/**
 * This UI is the application entry point. A UI may either represent a browser window
 * (or tab) or some part of a html page where a Vaadin application is embedded.
 * <p>
 * The UI is initialized using {@link #init(VaadinRequest)}. This method is intended to be
 * overridden to add component to the user interface and initialize non-component functionality.
 */
@Push(PushMode.MANUAL)
@Theme("mytheme")
public class MyUI extends UI {

    private static final long WAIT_STEP_MILLIS = 1000;
    private final HorizontalLayout mainLayout = new HorizontalLayout();
    private final VerticalLayout entryWrapLayout = new VerticalLayout();
    private final VerticalLayout membersWrapLayout = new VerticalLayout();
    private final VerticalLayout entryLayout = new VerticalLayout();
    private final VerticalLayout membersLayout = new VerticalLayout();
    private HazelcastService hzService;
    private Map<String, Member> memberMap = new ConcurrentHashMap<>();
    private Map<Integer, Partition> allPrimaryPartitionMap = new ConcurrentHashMap<>();
    private Map<Integer, Partition> allBackupPartitionMap = new ConcurrentHashMap<>();
    private int memberCounter = 0;
    private TextField keyField;
    private TextField valueField;
    private Label keyValueLabel;
    private Label hashcodeLabel;
    private ProgressBar bar;
    private Label arrowLabel;

    @Override
    protected void init(VaadinRequest vaadinRequest) {
        Button connectButton = new Button("Connect to the Hazelcast Cluster!");
        connectButton.addClickListener(event -> initUI());
        setContent(connectButton);
    }

    private void initUI() {
        if (!initHzService()) {
            System.out.println("No connection to the cluster!");
            return;
        }
        initEventService();
        mainLayout.setSizeFull();

        initWrapperLayouts();

        initEntryLayout();
        initMembersLayout();

        entryWrapLayout.addComponent(entryLayout);
        membersWrapLayout.addComponent(membersLayout);

        mainLayout.addComponents(entryWrapLayout, membersWrapLayout);
        mainLayout.setExpandRatio(entryWrapLayout, (float) 0.36);
        mainLayout.setExpandRatio(membersWrapLayout, (float) 0.64);

        setContent(mainLayout);
    }

    private void initWrapperLayouts() {
        entryWrapLayout.addStyleName(BACKGROUND_DARK_BLUE);
        entryWrapLayout.setSizeFull();
        entryWrapLayout.setSpacing(false);
        entryWrapLayout.setMargin(false);
        membersWrapLayout.addStyleName(BACKGROUND_DARK_BLUE);
        membersWrapLayout.setSizeFull();
        membersWrapLayout.setSpacing(false);
        membersWrapLayout.setMargin(false);
    }

    private void initEventService() {
        new Thread(new Runnable() {
            Set<HazelcastPartitioningEvent> events = new HashSet<>();

            @Override
            public void run() {
                IQueue<HazelcastPartitioningEvent> queue = hzService.getEventQueue();
                while (true) {
                    try {
                        HazelcastPartitioningEvent event = queue.take();
                        System.out.println("Consumed: " + event);
                        if (events.contains(event)) {
                            System.out.println("Same event!");
                            continue;
                        }
                        events.add(event);
                        MyUI.this.access(() -> {
                            bar.setVisible(true);
                            push();
                            MyUI.this.processEvent(event);
                            bar.setVisible(false);
                            push();
                        });
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    private void processEvent(HazelcastPartitioningEvent event) {
        System.out.println("Event arrived!");
        if (event instanceof MemberAddedEvent) {
            Member member = new Member(((MemberAddedEvent) event).getMemberAddress());
            member.setMemberName(Member.MEMBER_NAME_PREFIX + memberCounter++
                    + " " + member.getMemberAddress());
            memberMap.put(member.getMemberAddress(), member);
            membersLayout.addComponent(member);
            push();
            repartition();
            return;
        }
        if (event instanceof MemberRemovedEvent) {
            Member removedMember = memberMap.remove(((MemberRemovedEvent) event).getMemberAddress());
            membersLayout.removeComponent(removedMember);
            push();
            repartition();
            return;
        }
    }

    private void repartition() {
        String[][] partitionTable = hzService.getPartitionTable();
        provideSafety(partitionTable);
        migrate(partitionTable);
    }

    private void migrate(String[][] partitionTable) {
        for (int partitionId = 0; partitionId < partitionTable.length; partitionId++) {
            String[] replicas = partitionTable[partitionId];
            String primaryMemberAddress = replicas[0];
            String backupMemberAddress = replicas[1];
            waitAndPush();
            checkAllPrimariesAndMigrateTo(partitionId, primaryMemberAddress);
            waitAndPush();
            checkAllBackupsAndMigrateTo(partitionId, backupMemberAddress);
            push();
        }
    }

    private void provideSafety(String[][] partitionTable) {
        for (int partitionId = 0; partitionId < partitionTable.length; partitionId++) {
            String[] replicas = partitionTable[partitionId];
            String primaryMemberAddress = replicas[0];
            String backupMemberAddress = replicas[1];
            waitAndPush();
            checkAllPrimariesAndPutIfNotExists(partitionId);
            waitAndPush();
            if (backupMemberAddress == null) {
                memberMap.get(primaryMemberAddress).removeBackupPartition(allBackupPartitionMap.get(partitionId));
            } else {
                checkAllBackupsAndPutIfNotExists(partitionId);
            }
            push();
        }
    }

    private void waitAndPush() {
        try {
            Thread.sleep(WAIT_STEP_MILLIS);
            push();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void checkAllPrimariesAndMigrateTo(int partitionId, String primaryMemberAddress) {
        for (Member member : memberMap.values()) {
            if (!member.getMemberAddress().equals(primaryMemberAddress)
                    && member.containsInPrimaries(partitionId)) {
                Partition primary = allPrimaryPartitionMap.get(partitionId);
                member.removePrimaryPartition(primary);
                memberMap.get(primaryMemberAddress).addPrimaryPartition(primary);
                return;
            }
        }
    }

    private void checkAllBackupsAndMigrateTo(int partitionId, String backupMemberAddress) {
        for (Member member : memberMap.values()) {
            if (!member.getMemberAddress().equals(backupMemberAddress)
                    && member.containsInBackups(partitionId)) {
                Partition backup = allBackupPartitionMap.get(partitionId);
                member.removeBackupPartition(backup);
                memberMap.get(backupMemberAddress).addBackupPartition(backup);
                return;
            }
        }
    }

    private void checkAllBackupsAndPutIfNotExists(int partitionId) {
        for (Member member : memberMap.values()) {
            if (member.containsInBackups(partitionId)) {
                return;
            }
        }
        Partition backupPartition = allBackupPartitionMap.get(partitionId);
        Partition primaryPartition = allPrimaryPartitionMap.get(partitionId);
        memberMap.get(primaryPartition.getOwner()).addBackupPartition(backupPartition);
    }

    private void checkAllPrimariesAndPutIfNotExists(int partitionId) {
        for (Member member : memberMap.values()) {
            if (member.containsInPrimaries(partitionId)) {
                return;
            }
        }
        Partition primaryPartition = allPrimaryPartitionMap.get(partitionId);
        Partition backupPartition = allBackupPartitionMap.get(partitionId);
        memberMap.get(backupPartition.getOwner()).addPrimaryPartition(primaryPartition);
    }

    private boolean initHzService() {
        try {
            hzService = HazelcastService.getInstance();
            //clear the event queue from unwanted initial events
            hzService.getEventQueue().clear();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private void initEntryLayout() {
        entryLayout.setDefaultComponentAlignment(Alignment.MIDDLE_CENTER);

        String basepath = VaadinService.getCurrent()
                .getBaseDirectory().getAbsolutePath();
        FileResource resource = new FileResource(new File(basepath +
                "/WEB-INF/images/hazelcast-logo.png"));
        Image logo = new Image("", resource);
        entryLayout.addComponent(logo);

        Label titleLabel = new Label("Hazelcast Partitioning Demo");
        titleLabel.addStyleName(FONT_BOLD);
        titleLabel.addStyleName(FONT_20PX);
        titleLabel.addStyleName(COLOR_WHITE);
        titleLabel.addStyleName(TITLE_PADDING_BOTTOM);
        entryLayout.addComponent(titleLabel);

        HorizontalLayout prepareFormLayout = new HorizontalLayout();
        prepareFormLayout.setWidth(100, Unit.PERCENTAGE);
        keyField = new TextField();
        keyField.setWidth(100, Unit.PERCENTAGE);
        keyField.addStyleName(TEXT_ALIGNMENT_CENTER);
        keyField.setPlaceholder("Key");
        valueField = new TextField();
        valueField.setPlaceholder("Value");
        valueField.setWidthUndefined();
        valueField.addStyleName(TEXT_ALIGNMENT_CENTER);
        valueField.setWidth(100, Unit.PERCENTAGE);
        Button prepareButton = new Button("Display");
        prepareButton.addStyleName("button-icon");
        prepareButton.setIcon(VaadinIcons.MAGIC);
        prepareButton.addClickListener(e -> prepare());
        prepareButton.setWidth(100, Unit.PERCENTAGE);
        prepareFormLayout.addComponents(keyField, valueField, prepareButton);
        entryLayout.addComponent(prepareFormLayout);

        keyValueLabel = new Label();
        keyValueLabel.addStyleName(COLOR_WHITE);
        keyValueLabel.addStyleName(FONT_20PX);
        entryLayout.addComponent(keyValueLabel);

        arrowLabel = new Label();
        arrowLabel.setContentMode(ContentMode.HTML);
        arrowLabel.setValue(VaadinIcons.ARROW_CIRCLE_DOWN_O.getHtml());
        arrowLabel.setVisible(false);
        entryLayout.addComponent(arrowLabel);

        hashcodeLabel = new Label();
        hashcodeLabel.addStyleName(COLOR_WHITE);
        hashcodeLabel.addStyleName(FONT_20PX);
        entryLayout.addComponent(hashcodeLabel);

        Button putButton = new Button("Put!");
        putButton.addStyleName("button-icon");
        putButton.setIcon(VaadinIcons.DOWNLOAD);
        putButton.addClickListener(e -> put());
        entryLayout.addComponent(putButton);

        Label seperator = new Label();
        seperator.setContentMode(ContentMode.HTML);
        StringBuilder html = new StringBuilder();
        for (int i = 0; i < 7; i++) {
            html.append(VaadinIcons.LINE_H.getHtml());
        }
        seperator.setValue(html.toString());
        entryLayout.addComponent(seperator);

        HorizontalLayout getFormLayout = new HorizontalLayout();
        getFormLayout.setWidth(100, Unit.PERCENTAGE);
        TextField getKeyField = new TextField();
        getKeyField.setWidth(100, Unit.PERCENTAGE);
        getKeyField.addStyleName(TEXT_ALIGNMENT_CENTER);
        getKeyField.setPlaceholder("Key");
        Button getButton = new Button("Get!");
        getButton.addStyleName("button-icon");
        getButton.setWidth(100, Unit.PERCENTAGE);
        getButton.setIcon(VaadinIcons.CART);
        getFormLayout.addComponents(getKeyField, getButton);
        entryLayout.addComponent(getFormLayout);

        Label getResultLabel = new Label();
        getResultLabel.addStyleName(COLOR_WHITE);
        getResultLabel.addStyleName(FONT_20PX);
        entryLayout.addComponent(getResultLabel);

        getButton.addClickListener(e -> {
            String key = getKeyField.getValue();
            String value = hzService.get(key);
            if (value == null)
                value = "null";
            getResultLabel.setValue("[" + key + ", " + value + "]");
        });

        bar = new ProgressBar();
        bar.setIndeterminate(true);
        bar.setVisible(false);
        entryLayout.addComponent(bar);
    }

    private void put() {
        String key = keyField.getValue();
        int partitionId = hzService.getPartitionId(key);
        hzService.put(key, valueField.getValue());
        Entry primary = new Entry(key);
        Entry backup = new Entry(key);
        allPrimaryPartitionMap.get(partitionId).addEntry(primary);
        allBackupPartitionMap.get(partitionId).addEntry(backup);
    }

    private void prepare() {
        String key = keyField.getValue();
        String value = valueField.getValue();
        keyValueLabel.setValue("[" + key + ", " + value + "]");
        arrowLabel.setVisible(true);
        int pId = hzService.getPartitionId(key);
        hashcodeLabel.setValue("hashcode("
                + key
                + ") % partitionCount = "
                + pId);
        hashcodeLabel.setDescription(String.valueOf(key.hashCode()));
    }

    private void initMembersLayout() {
        membersLayout.setDefaultComponentAlignment(Alignment.TOP_CENTER);

        Label hazelcastClusterLabel = new Label("Hazelcast Cluster");
        hazelcastClusterLabel.addStyleName(COLOR_WHITE);
        hazelcastClusterLabel.addStyleName(FONT_20PX);
        membersLayout.addComponent(hazelcastClusterLabel);

        initPartitions(hzService.getPartitionCount());
        Collection<Member> members = hzService.getExistingMembers(allPrimaryPartitionMap, allBackupPartitionMap);
        memberCounter = members.size();
        for (Member member : members) {
            memberMap.put(member.getMemberAddress(), member);
            membersLayout.addComponent(member);
        }

//        initMockData();
    }

    private void initMockData() {
        for (int i = 0; i < 3; i++) {
            membersLayout.addComponent(new Member("Member #1 ([127.0.0.1]:5701)"), i);
        }
    }

    private void initPartitions(int partitionCount) {
        for (int i = 0; i < partitionCount; i++) {
            allPrimaryPartitionMap.put(i,
                    new Partition(i, Partition.PRIMARY_PARTITION_NAME_PREFIX + i, Partition.PRIMARY));
        }
        for (int i = 0; i < partitionCount; i++) {
            allBackupPartitionMap.put(i,
                    new Partition(i, Partition.BACKUP_PARTITION_NAME_PREFIX + i, Partition.BACKUP));
        }
    }

    @WebServlet(urlPatterns = "/*", name = "MyUIServlet", asyncSupported = true)
    @VaadinServletConfiguration(ui = MyUI.class, productionMode = false)
    public static class MyUIServlet extends VaadinServlet {
    }
}
