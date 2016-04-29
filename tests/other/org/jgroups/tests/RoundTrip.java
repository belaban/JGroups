package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Average;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

/**
 * Class that measure RTT for multicast messages between 2 cluster members. See {@link RpcDispatcherSpeedTest} for
 * RPCs
 * @author Bela Ban
 */
public class RoundTrip extends ReceiverAdapter {
    protected JChannel               channel;
    protected final Promise<Boolean> promise=new Promise<>();
    protected int                    num_msgs=5000;
    protected int                    msg_size=1000;
    protected boolean                oob, dont_bundle;


    protected void start(String props, String name) throws Exception {
        channel=new JChannel(props).name(name).setReceiver(this);
        channel.connect("rt");
        View view=channel.getView();
        if(view.size() > 2)
            System.err.printf("More than 2 members found (%s); terminating\n", view);
        else
            loop();
        Util.close(channel);
    }

    /**
     * On the server: receive a request, send a response. On the client: send a request, wait for the response
     * @param msg
     */
    public void receive(Message msg) {
        if(msg.getLength() > 0) {       // request: send unicast response
            Message rsp=new Message(msg.src());
            try {
                channel.send(rsp);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        else
            promise.setResult(true);
    }

    public void viewAccepted(View view) {
        System.out.println("view = " + view);
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_msgs (%d) [3] msg_size (%d) [o] oob (%b) [b] dont_bundle (%b)[x] exit\n",
                                              num_msgs, msg_size, oob, dont_bundle));
            try {
                switch(c) {
                    case '1':
                        sendRequests();
                        break;
                    case '2':
                        num_msgs=Util.readIntFromStdin("num_msgs: ");
                        break;
                    case '3':
                        int tmp=Util.readIntFromStdin("msg_size: ");
                        if(tmp <= 0)
                            System.err.printf("msg_size of %d is invalid\n", tmp);
                        else
                            msg_size=tmp;
                        break;
                    case 'o':
                        oob=!oob;
                        break;
                    case 'b':
                        dont_bundle=!dont_bundle;
                        break;
                    case 'x':
                        looping=false;
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
    }

    protected void sendRequests() throws Exception {
        View view=channel.getView();
        if(view.size() != 2) {
            System.err.printf("Cluster must have exactly 2 members: %s\n", view);
            return;
        }

        Address target=Util.pickNext(view.getMembers(), channel.getAddress());
        byte[] buf=new byte[msg_size];
        Average avg=new Average();
        long min=Long.MAX_VALUE, max=0;
        int print=num_msgs/10;
        System.out.printf("-- sending %d requests to %s (size=%d)\n", num_msgs, target, msg_size);

        for(int i=0; i < num_msgs; i++) {
            Message req=new Message(target, buf);
            if(oob)
                req.setFlag(Message.Flag.OOB);
            if(dont_bundle)
                req.setFlag(Message.Flag.DONT_BUNDLE);
            promise.reset(false);
            long start=System.nanoTime();
            channel.send(req);
            if(i > 0 && i % print == 0)
                System.out.print(".");
            promise.getResult(0);
            long time_ns=System.nanoTime()-start;
            avg.add(time_ns);
            min=Math.min(min, time_ns);
            max=Math.max(max, time_ns);
        }
        System.out.println("");
        System.out.printf("\nround-trip = min/avg/max: %.2f / %.2f / %.2f us\n\n", min/1000.0, avg.getAverage() / 1000.0, max/1000.0);
    }



    public static void main(String[] args) throws Exception {
        String props=null, name=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }
        new RoundTrip().start(props, name);
    }



    private static void help() {
        System.out.println("RoundTrip [-props <properties>] [-name name]");
    }
}
