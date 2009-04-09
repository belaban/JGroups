package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Passes a token between members as soon as it can, or with -sleep_time ms in-between. Currently, token recovery (on
 * member leaving is very crude). This test measures latency between token rotations across the ring. To get good
 * numbers, disable message bundling in the transport, or set max_bundle_time to a very low value.
 * @author Bela Ban
 * @version $Id: TokenTest.java,v 1.2 2009/04/09 09:11:20 belaban Exp $
 */
public class TokenTest extends ReceiverAdapter {
    JChannel ch;
    Address  next, local_addr;
    final List<Address> members=new ArrayList<Address>(10);
    int num_tokens=0;
    long last_token_timestamp, sleep_time=0;
    boolean token_sent=false;
    private final Recovery recovery=new Recovery();

    private void start(String[] args) throws ChannelException {
        String props="udp.xml";
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-sleep")) {
                sleep_time=Long.valueOf(args[++i]);
                continue;
            }
            System.out.println("TokenTest [-props <props>] [-sleep <sleep time between token sends>]");
            return;
        }

        ch=new JChannel(props);
        ch.setReceiver(this);
        ch.connect("token-group");
        local_addr=ch.getAddress();
        next=determineNext(members, local_addr);
        if(isCoord() && !token_sent) {
            ch.send(next, null, null);
            token_sent=true;
        }
        recovery.start();
    }




    public void receive(Message msg) {
        // System.out.println("received token from " + msg.getSrc());
        num_tokens++;
        if(last_token_timestamp == 0) {
            last_token_timestamp=System.currentTimeMillis();
        }
        else {
            if(num_tokens % 1000 == 0) {
                long diff=System.currentTimeMillis() - last_token_timestamp;
                System.out.println("round trip time=" + diff + " ms");
            }
        }
        try {
            if(sleep_time > 0) {
                // System.out.println("sleeping for " + sleep_time + " ms");
                Util.sleep(sleep_time);
            }
            // System.out.println("[" + local_addr + "]: sending token to " + next);

            while(!ch.isConnected()) {
                Util.sleep(10);
            }

            last_token_timestamp=System.currentTimeMillis();
            ch.send(next, null, null);
            System.out.print(num_tokens + "\r");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void viewAccepted(View new_view) {
        System.out.println("view: " + new_view);
        members.clear();
        members.addAll(new_view.getMembers());
        next=determineNext(members, local_addr);

        if(local_addr != null && isCoord() && !token_sent) {
            try {
                ch.send(next, null, null);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            token_sent=true;
        }
    }

    private boolean isCoord() {
        return members.get(0).equals(local_addr);

    }


    private static Address determineNext(List<Address> members, Address local_addr) {
        int index=members.indexOf(local_addr);
        Address retval=members.get(++index % members.size());
        if(retval == null)
            retval=local_addr;
        return retval;
    }


    private class Recovery implements Runnable {

        void start() {
            new Thread(this).start();
        }

        public void run() {
            for(;;) {
                Util.sleep(5000);
                if(last_token_timestamp > 0 && System.currentTimeMillis() - last_token_timestamp > 5000) {
                    if(local_addr != null && isCoord()) {
                        try {
                            System.out.println("*** recovering token");
                            ch.send(next, null, null);
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                        token_sent=true;
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws ChannelException {
        new TokenTest().start(args);
    }
}
