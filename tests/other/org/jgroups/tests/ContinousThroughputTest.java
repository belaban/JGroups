package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolObserver;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Date;


/**
 * <h1>ContinousThroughputTest.java</h1>
 * <p/>
 * This is a program to make Throughput tests.
 * <p/>
 * The program assumes to run on a reliable network where no partitioning or failures happen (Apart for cping test).
 * Once you run the program it connects the channel and gives you a prompt.
 * Every time a new view is received you will see it printed.
 * Once you have launched the program on all the machine you use for the test just digit
 * on one machine the command for the test you desire to make, you will be asked for the necessary parameters,
 * then the test starts.
 * Depending on the chosen test you will see its results on the monitor and them ar logged
 * on a file on the working dir called <code>"ContinousThroughputTest<hostname><systemTimeInSeconds>.log"</code> .
 *
 * @author Gianluca Collot
 * @version 1.0
 */

public class ContinousThroughputTest {
    String props="UDP:" +
            "PING(up_thread=false;down_thread=false):" +
            "FD(timeout=1000;shun=false):" +
            "STABLE(up_thread=false;down_thread=false):" +
            "MERGE(up_thread=false;down_thread=false):" +
            "NAKACK:" +
            "FLUSH:" +
            "GMS:" +
            "VIEW_ENFORCER(up_thread=false;down_thread=false):" +
//		"TSTAU:" +
            "QUEUE(up_thread=false;down_thread=false)";
//  String props= "TCP:TCPPING(initial_hosts=manolete2[8880]):FD(timeout=10000):" +
//		"STABLE:MERGE:NAKACK:FRAG:FLUSH:GMS:VIEW_ENFORCER:QUEUE";
    JChannel channel=null;
    Thread sendThread, receiveThread;
    boolean coordinator=false;
    IpAddress my_addr=null;
    View view;
    BufferedReader reader;
    float troughputSum=0, meanTroughput=0, minTroughput=10000, maxTroughput=0;
    int numTests=0;
    FileWriter logWriter;
    Protocol prot=null;

    /**
     * Creates threads, creates and connects channel opens log file
     */

    public ContinousThroughputTest() {
        sendThread=new Thread("sendThread") {
            public void run() {
                parser();
            }
        };
        receiveThread=new Thread("receiveThread") {
            public void run() {
                checkChannel();
            }
        };
        reader=new BufferedReader(new InputStreamReader(System.in));
        try {
            channel=new JChannel(props);
//      prot = (Protocol) channel.getProtocolStack().getProtocols().lastElement();
//      prot.setObserver(new ContinousThroughputTest.MessageLenghtObserver());
            channel.setOpt(Channel.BLOCK, Boolean.FALSE);
            channel.setOpt(Channel.SUSPECT, Boolean.FALSE);
            channel.connect("Janus");
        }
        catch(Exception ex) {
            System.out.println("Connection Failed!" + ex);
            System.exit(1);
        }
        my_addr=(IpAddress)channel.getLocalAddress();

        try {
            File log=new File("ContinousThroughputTest" + my_addr.getIpAddress().getHostName()
                    + (System.currentTimeMillis() / 10000) + ".log");
            if(!log.exists()) {
                log.createNewFile();
            }
            logWriter=new FileWriter(log);
            logWriter.write("ContinousThroughputTest.java log\r\n");
            logWriter.write("Date:" + new Date(System.currentTimeMillis()) + "\r\n");
            log("Protocol Stack is " + props);
            System.out.println("Protocol Stack is " + props);
        }
        catch(Exception ex) {
            System.out.println("File problems " + ex);
            System.exit(5);
        }
    }

    static void main(String[] args) {
        ContinousThroughputTest perfTest=new ContinousThroughputTest();
        perfTest.go();
    }

    void go() {
//    Starts Receiving
        receiveThread.start();
//    Starts input Parser
        sendThread.start();
    }

    /**
     * This function should be called in its own thread.
     * It recives messages and calculates the troughput
     */

    public void checkChannel() {
        String payload=null;
        Object received=null;
        Message msg=null;
        boolean done=false;
        long n;
        int i=1;

        System.out.println("Started receiving");
        try {
            while(!done) {
                received=channel.receive(0);
                if(received instanceof Message) {
                    msg=(Message)received;
                    payload=(String)msg.getObject();
                    System.out.println(payload);
                    if("stop".equalsIgnoreCase(payload)) {
                        done=true;
                    }
                    if("pingpong".equalsIgnoreCase(payload)) {
                        n=((Long)((Message)channel.receive(0)).getObject()).longValue();
                        i=((Integer)((Message)channel.receive(0)).getObject()).intValue();
                        log("Starting pingpong test. Rounds: " + n + " Bursts: " + i);
                        pingpongTest(n, i, false);
                    }
                    if("cping".equalsIgnoreCase(payload)) {
//	    i = ((Integer) ((Message) channel.receive(0)).getObject()).intValue();
                        log("Starting cping test. Bursts: " + 1);
                        cpingTest(1, true);
                    }
                    if("sweep".equalsIgnoreCase(payload)) {
                        n=((Long)((Message)channel.receive(0)).getObject()).longValue();
                        i=((Integer)((Message)channel.receive(0)).getObject()).intValue();
                        log("Starting sweep test. Rounds: " + n + " initial burst: " + i);
                        sweep(n, i);
                    }
                }
                if(received instanceof View) {
                    view=(View)received;
                    System.out.println(view);
                    if(view.getMembers().elementAt(0).equals(my_addr)) {
                        System.out.println("I'm the new Coordinator");
                        coordinator=true;
                    }
                    resetData();
                }
            }
        }
        catch(Exception ex) {
            System.out.println("checkChannel() :" + ex);
            try {
                logWriter.write("Stopped cause " + ex + "\r\n");
            }
            catch(Exception e) {
            }
            System.exit(2);
        }
        System.out.println("Stopped Receiving");

        channel.disconnect();
        System.out.println("Disconnected from \"Janus\"");
        channel.close();
        System.out.println("Channel Closed");
        System.exit(0);
    }

    /**
     * This function should be run in its own thread and sends messages on an already connected channel
     */
    public void parser() {
        boolean done=false;
        String input;
        int number=0;
        int burstlength=1;

        System.out.println("Ready.");
        try {
            while(!done) {
                input=reader.readLine();
                if("stop".equalsIgnoreCase(input)) {
                    done=true;
                }
                if("pingpong".equalsIgnoreCase(input)) {
                    number=askNumber(reader, "How many rounds?");
                    burstlength=askNumber(reader, "Length of bursts?");
                    channel.send(new Message(null, null, input));
                    channel.send(new Message(null, null, new Long(number)));
                    channel.send(new Message(null, null, new Integer(burstlength)));
                    continue;

                }
                if("cping".equalsIgnoreCase(input)) {
//	       burstlength = askNumber( reader,"Length of bursts?");
                    channel.send(new Message(null, null, input));
//	       channel.send(new Message(null,null,new Integer(burstlength)));
                    continue;
                }
                if("sweep".equalsIgnoreCase(input)) {
                    number=askNumber(reader, "Number of tests");
                    burstlength=askNumber(reader, "Initial length of bursts?");
                    channel.send(new Message(null, null, input));
                    channel.send(new Message(null, null, new Long(number)));
                    channel.send(new Message(null, null, new Integer(burstlength)));
                    continue;
                }
                channel.send(new Message(null, null, input));
            }
        }
        catch(Exception ex) {
            System.out.println(ex);
        }
    }

    /**
     * sendBurst(int n): sends a burst of messages with small payload
     */

    void sendBurst(long n) {
        try {
            byte[] buf=Util.objectToByteBuffer("Standard Mex");
            for(int i=0; i < n; i++) {
                channel.send(new Message(null, null, buf));
            }
        }
        catch(Exception ex) {
            System.out.println("sendBurst: " + ex);
        }
    }


    /**
     * showStats: Prints resulting times and troughput
     */

    void showStats(long start, long stop, long messages, int burstlength) {
        String result;
        long elapsedTime=(stop - start);
        long troughPut=(messages * 1000) / elapsedTime;
//    troughputSum += troughPut;
        maxTroughput=(maxTroughput > troughPut) ? maxTroughput : troughPut;
        minTroughput=(minTroughput < troughPut) ? minTroughput : troughPut;
//    System.out.println("Elapsed Time: " + (stop-start) + " milliseconds to receive " + messages + " messages");
        result="Elapsed Time: " + (stop - start) +
                "| messages:" + messages +
                "| burst length:" + burstlength +
                "| Troughput:" + troughPut +
                "| max: " + maxTroughput +
                "| min: " + minTroughput +
                "\r\n";
        System.out.println(result);
        try {
            logWriter.write(result);
            logWriter.flush();
        }
        catch(Exception ex) {
            System.out.println("showStats():" + ex);
        }

    }

    int askNumber(BufferedReader reader, String text) {
        int number=0;
        String input="10";
        System.out.println(text);
        try {
            input=reader.readLine();
        }
        catch(Exception ex) {
            System.out.println("AskNumber :" + ex);
        }

        number=Integer.parseInt(input);
        return number;
    }

    /**
     * Resets stored statistics and counters
     */

    void resetData() {
        maxTroughput=0;
        minTroughput=10000;
        meanTroughput=0;
        numTests=0;
        troughputSum=0;
    }

    /**
     * Make a pingpong test:
     * For n times a message is sent and view.size() messages are received
     * Every 1000 messages sent the throughput is evaluated or at the end of the test
     */
    void pingpongTest(long n, int burst_length, boolean partialResultsPrint) {
        long i=0;
        long start=System.currentTimeMillis();
        long tempstart=System.currentTimeMillis();
        long stop, throughput;
        try {
            for(i=0; i < n; i++) {
                for(int k=0; k < burst_length; k++)
                    channel.send(new Message(null, null, new Long(i)));
                for(int j=0; j < (view.size() * burst_length); j++) {
                    channel.receive(20000);
                }
                if(partialResultsPrint && ((i % 1000) == 0)) {
                    if(i == 0) continue;
                    stop=System.currentTimeMillis();
                    throughput=(1000000 / (stop - tempstart)) * view.size() * burst_length;
                    try {
                        System.out.println(new Date(stop).toString() + " : " + throughput);
                        logWriter.write(new Date(stop).toString() + " : " + throughput);
                        logWriter.write("\r\n");
                        logWriter.flush();
                        tempstart=System.currentTimeMillis();
                    }
                    catch(Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        catch(TimeoutException ex) {
            System.out.println("Timeout Receiving, round: " + i);
            System.exit(5);
        }
        catch(Exception ex) {
            ex.printStackTrace();
            System.exit(4);
        }
        stop=System.currentTimeMillis();
        showStats(start, stop, n * view.size() * burst_length, burst_length);
    }

    void sweep(long tests, int burstlenght) {
        long messagespertest=10000;
        for(int i=0; i < tests; i++) {
            burstlenght+=i;
            pingpongTest(messagespertest / burstlenght, burstlenght, false);
        }
    }

    /**
     * Makes a continous test handling view changes
     */
    void cpingTest(int burst_lenght, boolean printoutput) {
        Object recvd=null;
        long start=System.currentTimeMillis();
        for(long i=1; i < Long.MAX_VALUE; i++) {
//	 System.out.println("Round: " + i);
            try {
                channel.send(null, null, "cping");
                for(int j=0; j < burst_lenght * view.size();) {
                    recvd=channel.receive(10000);
                    if(recvd instanceof View) {
                        view=(View)recvd;
                        System.out.println(view);
                        log(view.toString());
                    }
                    else {
                        j++;
                    }
                }
            }
            catch(TimeoutException tex) {
                try {
                    channel.send(new Message(null, null, "cping"));
                    System.out.println("Resent a message for timeout");
                    log("Resent a message for timeout");
                }
                catch(Exception ex) {
                    System.exit(9);
                }
            }
            catch(Exception ex) {
                System.exit(9);
            }
            if((i % 1000) == 0) {
                long stop=System.currentTimeMillis();
                long throughput=i * 1000 * view.size() / (stop - start);
                System.out.println("Througputh = " + throughput);
                log("Througputh = " + throughput);
                start=System.currentTimeMillis();
                i=0;
            }
        }
    }

    /**
     * Used to print messages lenght and their serialized contents.
     */

    public static class MessageLenghtObserver implements ProtocolObserver {

        public void setProtocol(Protocol prot) {
            /** todo: Implement this org.jgroups.debug.ProtocolObserver method*/
            throw new java.lang.UnsupportedOperationException("Method setProtocol() not yet implemented.");
        }

        public boolean up(Event evt, int num_evts) {
            /** todo: Implement this org.jgroups.debug.ProtocolObserver method*/
            throw new java.lang.UnsupportedOperationException("Method up() not yet implemented.");
        }

        public boolean passUp(Event evt) {
            return true;
        }

        public boolean down(Event evt, int num_evts) {
            return true;
        }

        public boolean passDown(Event evt) {
            byte[] buf=null;
            if(evt.getType() == Event.MSG)
                try {
                    buf=Util.objectToByteBuffer(evt.getArg());
                    System.out.println("UDP: sending a message of " +
                            buf.length +
                            "bytes");
                    System.out.println("Message was :");
                    System.out.println(new String(buf));
                }
                catch(Exception ex) {

                }
            return true;
        }
    }

    void log(String str) {
        try {
            logWriter.write(str + "\r\n");
            logWriter.flush();
        }
        catch(Exception ex) {

        }
    }
}
