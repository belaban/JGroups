package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.AnycastAddress;
import org.jgroups.util.Util;

import javax.management.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.*;

/**
 * Runs the Total Order Anycast protocol and saves the messages delivered
 * 
 * Note: this is used for debugging
 * Note2: this needs to be clean :)
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class TestToaOrder {
    private static final String PROPS = "toa.xml";
    private static final String CLUSTER = "test-toa-cluster";
    private static final String OUTPUT_FILE_SUFFIX = "-messages.txt";
    private static final String JMX_DOMAIN = "org.jgroups";

    private JChannel jChannel;
    private MyReceiver receiver;
    private int numberOfNodes;
    private int numberOfMessages;
    private final List<Address> members = new LinkedList<>();

    private long start;
    private long stop;
    private long sentBytes = 0;
    private long sentMessages = 0;
    private String config;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("==============");
        System.out.println("Test TOA Order");
        System.out.println("==============");


        ArgumentsParser argumentsParser = new ArgumentsParser(args);
        if (argumentsParser.isHelp()) {
            helpAndExit();
        } else if(argumentsParser.isTestOrder()) {
            /*String[] paths = argumentsParser.getFilesPath();
            int numberOfFiles = paths.length;

            ProcessFile[] threads = new ProcessFile[numberOfFiles];

            for (int i = 0; i < threads.length; ++i) {
                threads[i] = new ProcessFile(paths[i]);
                threads[i].start();
            }

            Map<String, MessageInfo> allMessages = new HashMap<String, MessageInfo>();
            for (ProcessFile processFile : threads) {
                processFile.join();
                for (MessageInfo messageInfo : processFile.list) {
                    String message = messageInfo.message;
                    if (!allMessages.containsKey(message)) {
                        allMessages.put(message, messageInfo);
                    } else {
                        allMessages.get(message).join(messageInfo);
                    }
                }
            }

            for (MessageInfo messageInfo : allMessages.values()) {
                messageInfo.check();
            }
            System.out.println("============= FINISHED =============");
            System.exit(0);*/
        }

        TestToaOrder test = new TestToaOrder(
                argumentsParser.getNumberOfNodes(),
                argumentsParser.getNumberOfMessages(),
                argumentsParser.getConfig());

        try {
            test.startTest();
        } catch (Exception e) {
            System.err.println("Error while executing the test: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            test.closeJChannel();
            System.out.println("============= FINISHED =============");
        }
        System.exit(0);
    }

    private static void helpAndExit() {
        System.out.println("usage: " + TestToaOrder.class.getCanonicalName() + " <option>");
        System.out.println("Options:");
        System.out.println("  -h                    \tshow this message");
        System.out.println("  -nr-nodes <value>     \tnumber of nodes");
        System.out.println("  -nr-messages <values> \tnumber of messages to send by each node");
        System.out.println("  -config <file>        \tthe JGroup's configuration file");
        System.exit(1);
    }

    // ====================== arguments parser ======================

    private static class ArgumentsParser {
        private static final int NR_NODES = 4;
        private static final int NR_MESSAGES = 1000;

        private String[] args;

        private int numberOfNodes = -1;
        private int numberOfMessages = -1;
        private boolean help = false;
        private boolean testOrder = false;
        private String[] filesPath = null;
        private String config = PROPS;

        public ArgumentsParser(String[] args) {
            this.args = args;
            parse();
            checkConfig();
        }

        private void parse() {
            try {
                for (int i = 0; i < args.length; ++i) {
                    if ("-h".equals(args[i])) {
                        help = true;
                    } else if ("-nr-nodes".equals(args[i])) {
                        numberOfNodes = Integer.parseInt(args[++i]);

                        if (numberOfNodes < NR_NODES) {
                            System.err.println("Number of nodes must be greater or equal to " + NR_NODES);
                            System.exit(1);
                        }
                    } else if ("-nr-messages".equals(args[i])) {
                        numberOfMessages = Integer.parseInt(args[++i]);

                        if (numberOfMessages <= 0) {
                            System.err.println("Number of messages must be greater than 0");
                            System.exit(1);
                        }
                    } else if ("-config".equals(args[i])) {
                        config = args[++i];
                    } else {
                        System.err.println("Unknown argument: " +args[i]);
                        helpAndExit();
                    }
                }
            } catch (Throwable t) {
                System.err.println("Error processing arguments: " + t.getMessage());
                t.printStackTrace();
                System.exit(1);
            }
        }

        private void checkConfig() {
            if (numberOfNodes == -1) {
                numberOfNodes = NR_NODES;
            }
            if (numberOfMessages == -1) {
                numberOfMessages = NR_MESSAGES;
            }
        }

        public boolean isHelp() {
            return help;
        }

        public boolean isTestOrder() {
            return testOrder;
        }

        public int getNumberOfNodes() {
            return numberOfNodes;
        }

        public int getNumberOfMessages() {
            return numberOfMessages;
        }

        public String[] getFilesPath() {
            return filesPath;
        }

        public String getConfig() {
            return config;
        }
    }

    // ====================== receiver ======================

    private static class MyReceiver extends ReceiverAdapter {
        private int expectedMembers;
        private int members = 0;
        private final List<String> messageList;
        private final TestToaOrder testGroupMulticastOrder;

        private long start = 0;
        private long stop = 0;
        private long receivedBytes = 0;
        private int receivedMsgs = 0;

        public MyReceiver(int expectedMembers, TestToaOrder testGroupMulticastOrder) {
            this.expectedMembers = expectedMembers;
            this.testGroupMulticastOrder = testGroupMulticastOrder;
            this.messageList = new LinkedList<>();
        }

        @Override
        public void receive(Message msg) {
            DataMessage dataMessage = (DataMessage) msg.getObject();
            switch (dataMessage.type) {
                case DataMessage.FINISH:
                    testGroupMulticastOrder.memberFinished(msg.getSrc());
                    break;
                case DataMessage.DATA:
                    if (start == 0) {
                        start = System.nanoTime();
                    }
                    synchronized (messageList) {
                        messageList.add(dataMessage.data);
                    }
                    receivedBytes += (dataMessage.data.getBytes().length + 1);
                    receivedMsgs++;
                    stop = System.nanoTime();
                    break;
                default:
                    break;
            }
        }

        @Override
        public void viewAccepted(View view) {
            System.out.println("New View: " + view);
            super.viewAccepted(view);
            synchronized (this) {
                members = view.getMembers().size();
                this.notify();
            }
        }

        public synchronized void waitUntilClusterIsFormed() throws InterruptedException {
            while (members < expectedMembers) {
                System.out.println("Number of members is not the expected: " + members + " of " + expectedMembers);
                this.wait();
            }
        }

        public void await(int expectedMessages) throws InterruptedException {
            int actualSize;
            while (true) {
                synchronized (messageList) {
                    actualSize = messageList.size();
                }
                if (actualSize < expectedMessages) {
                    System.out.println("waiting messages... " + actualSize + " of " + expectedMessages);
                    Thread.sleep(10000);
                } else {
                    break;
                }
            }
        }

        public List<String> getMessageList() {
            return messageList;
        }

        public void printReceiverInfo() {
            System.out.println("+++ Receiver Information +++");
            double duration = stop - start;
            duration /= 1000000.0; //nano to millis
            System.out.println("+ Duration (msec)   = " + duration);
            System.out.println("+ Received Bytes    = " + receivedBytes);
            System.out.println("+ Received Messages = " + receivedMsgs);
            duration /= 1000.0; //millis to sec
            System.out.println("---------------------");
            System.out.println("+ Receiving Throughput (bytes/sec)  = " + (receivedBytes / duration));
            System.out.println("+ Receiving Messages (messages/sec) = " + (receivedMsgs / duration));
            System.out.println("-------------------------------------");
        }

    }

    // ====================== messages info (deliver before and after) ================
    /*private static class MessageInfo {
        private String message;
        private Set<String> deliveredBefore = new HashSet<String>();
        private Set<String> deliveredAfter = new HashSet<String>();

        public void join(MessageInfo messageInfo) {
            this.deliveredAfter.addAll(messageInfo.deliveredAfter);
            this.deliveredBefore.addAll(messageInfo.deliveredBefore);
        }

        public void check() {
            Set<String> intersect = new HashSet<String>(deliveredBefore);
            intersect.retainAll(deliveredAfter);
            if (!intersect.isEmpty()) {
                System.err.println("ERROR: WRONG ORDER! messages " + intersect + " was delivered before and after this" +
                        " message " + message);
            }
        }
    }
    
    private static class MessageInfo2 extends MessageInfo {
        private String message;
        private Set<MessageInfo2> deliveredBefore = new HashSet<MessageInfo2>();
        
        @Override
        public void join(MessageInfo messageInfo) {
            deliveredBefore.addAll(((MessageInfo2)messageInfo).deliveredBefore);
        }
        
        @Override
        public void check() {
            for (MessageInfo2 messageInfo2 : deliveredBefore) {
                if (messageInfo2.deliveredBefore.contains(this)) {
                    System.err.println("ERROR: WRONG ORDER! This message " + message + " was delivered before and after the" +
                        " message " + messageInfo2.message);
                }
            }
        }
    }*/

    //======================= thread processing each input file =====================
    /*private static class ProcessFile extends Thread {
        private String filepath;
        public List<MessageInfo> list = new LinkedList<MessageInfo>();

        private ProcessFile(String filepath) {
            super();
            this.filepath = filepath;
        }

        @Override
        public void run() {
            runV2();
        }
        
        public void runV1() {
            try {
                Set<String> previously = new HashSet<String>();
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filepath));
                String message;

                while ((message = bufferedReader.readLine()) != null) {
                    MessageInfo messageInfo = new MessageInfo();
                    messageInfo.message = message;
                    messageInfo.deliveredBefore.addAll(previously);

                    for (MessageInfo aux : list) {
                        aux.deliveredAfter.add(message);
                    }
                    list.add(messageInfo);
                    previously.add(message);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            } catch (IOException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            }
        }
        
        public void runV2() {
            try {
                Set<MessageInfo2> previously = new HashSet<MessageInfo2>();
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filepath));
                String message;

                while ((message = bufferedReader.readLine()) != null) {
                    MessageInfo2 messageInfo = new MessageInfo2();
                    messageInfo.message = message;
                    messageInfo.deliveredBefore.addAll(previously);                    
                    list.add(messageInfo);
                    previously.add(messageInfo);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            } catch (IOException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            }
        }
    }*/

    //======================= data message =======================
    private static class DataMessage implements Serializable {
        public transient static final byte FINISH = 1; //1 << 0
        public transient static final byte DATA = 1 << 1;
        private static final long serialVersionUID=5946678490588947910L;

        private byte type;
        private String data;
    }

    // ====================== other methods ======================

    public TestToaOrder(int numberOfNodes, int numberOfMessages, String config) {
        this.numberOfNodes = numberOfNodes;
        this.numberOfMessages = numberOfMessages;
        this.config = config;
    }

    private void createJChannel() throws Exception {
        System.out.println("Creating Channel");
        receiver = new MyReceiver(numberOfNodes, this);
        jChannel = new JChannel(config);

        jChannel.setReceiver(receiver);
        jChannel.connect(CLUSTER);

        receiver.waitUntilClusterIsFormed();
        Util.registerChannel(jChannel, JMX_DOMAIN);

        members.addAll(jChannel.getView().getMembers());
    }

    private AnycastAddress getDestinations(List<Address> members) {
        int rand = members.indexOf(jChannel.getAddress());

        AnycastAddress address = new AnycastAddress();

        address.add(members.get(rand++ % members.size()),
                    members.get(rand++ % members.size()),
                    members.get(rand % members.size()));

        return address;
    }

    private void sendMessages() throws Exception {
        System.out.println("Start sending messages...");

        String address = jChannel.getAddressAsString();
        List<Address> mbrs = jChannel.getView().getMembers();
        start = System.nanoTime();
        for (int i = 0; i < numberOfMessages; ++i) {
            AnycastAddress dst = getDestinations(mbrs);
            Message message = new BytesMessage().setDest(dst);

            DataMessage dataMessage = new DataMessage();
            dataMessage.type = DataMessage.DATA;
            dataMessage.data = address + ":" + i;

            message.setObject(dataMessage);
            jChannel.send(message);

            sentBytes += (dataMessage.data.getBytes().length + 1);
            sentMessages++;
        }
        stop = System.nanoTime();

        System.out.println("Finish sending messages...");
    }

    private void awaitUntilAllMessagesAreReceived() throws InterruptedException {
        int expectedMessages = 3 * numberOfMessages;

        receiver.await(expectedMessages);
    }

    private void awaitUntilAllFinishes() throws Exception {
        DataMessage dataMessage = new DataMessage();
        dataMessage.type = DataMessage.FINISH;
        dataMessage.data = null;

        jChannel.send(null, dataMessage);

        synchronized (members) {
            if (!members.isEmpty()) {
                members.wait();
            }
        }
    }

    public void printSenderInfo() {
        System.out.println("+++ Sender Information +++");
        double duration = stop - start;
        duration /= 1000000.0; //nano to millis
        System.out.println("+ Duration (msec) = " + duration);
        System.out.println("+ Sent Bytes      = " + sentBytes);
        System.out.println("+ Sent Messages   = " + sentMessages);
        duration /= 1000.0; //millis to sec
        System.out.println("-------------------");
        System.out.println("+ Sent Throughput (bytes/sec)  = " + (sentBytes / duration));
        System.out.println("+ Sent Messages (messages/sec) = " + (sentMessages / duration));
        System.out.println("--------------------------------");
    }

    public void memberFinished(Address addr) {
        synchronized (members) {
            members.remove(addr);
            if (members.isEmpty()) {
                members.notify();
            }
        }
    }

    public void closeJChannel() {
        System.out.println("Close channel");
        jChannel.close();
    }

    public void startTest() throws Exception {
        System.out.println("Start testing...");
        createJChannel();
        sendMessages();
        awaitUntilAllMessagesAreReceived();

        String filePath = jChannel.getAddressAsString() + OUTPUT_FILE_SUFFIX;
        System.out.println("Writing messages in " + filePath);

        FileWriter fileWriter = new FileWriter(filePath);
        for (String s : receiver.getMessageList()) {
            fileWriter.write(s);
            fileWriter.write("\n");
        }
        fileWriter.flush();
        fileWriter.close();
        System.out.println("All done!");

        awaitUntilAllFinishes();

        printSenderInfo();
        receiver.printReceiverInfo();
        printJMXStats();
    }

    private static void printJMXStats() {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName groupMulticast = getGroupMulticastObjectName(mBeanServer);

        if (groupMulticast == null) {
            System.err.println("Unable to find the GROUP_MULTICAST protocol");
            return ;
        }

        try {
            System.out.println("======== JMX STATS =========");
            for (MBeanAttributeInfo mBeanAttributeInfo : mBeanServer.getMBeanInfo(groupMulticast).getAttributes()) {
                String attribute = mBeanAttributeInfo.getName();
                String type = mBeanAttributeInfo.getType();

                if (!type.equals("double") && !type.equals("int")) {
                    continue;
                }

                System.out.println(attribute + "=" + mBeanServer.getAttribute(groupMulticast, attribute));
            }
            System.out.println("======== JMX STATS =========");
        } catch (Exception e) {
            System.err.println("Error collecting stats" + e.getLocalizedMessage());
        }
    }

    private static ObjectName getGroupMulticastObjectName(MBeanServer mBeanServer) {
        for(ObjectName name : mBeanServer.queryNames(null, null)) {
            if(name.getDomain().equals(JMX_DOMAIN)) {
                if ("TOA".equals(name.getKeyProperty("protocol"))) {
                    return name;
                }
            }
        }
        return null;
    }
}
