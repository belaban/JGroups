// $Id: QuoteServer.java,v 1.9 2006/05/03 08:14:00 belaban Exp $

package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;




/**
 * Example of a replicated quote server. The server maintains state which consists of a list
 * of quotes and their corresponding values. When it is started, it tries to reach other
 * quote servers to get its initial state. If it does not receive any response after 5
 * seconds, it assumes it is the first server and starts processing requests. When it
 * receives a view notification it checks whether there are more members in the view than in
 * its previous view. If this is the case, it broadcasts a request for the state to all
 * members. Integration of different states is simply the union of all states (with the
 * danger of overwriting mutually inconsistent state).<p> This mechanism allows both for
 * fast discovery of initial servers, and - in the case of partition merging - for
 * reintegration of existing servers. Broadcasting the state request upon every view change
 * (only when new members are joined) causes potentially a lot of network traffic, but it is
 * assumes that there will not be more than 5 quote servers at most.
 * @author Bela Ban
 */

public class QuoteServer implements MembershipListener, MessageListener {
    final Hashtable stocks=new Hashtable();
    Channel channel;
    RpcDispatcher disp;
    static final String channel_name="Quotes";
    final int num_members=1;
    Log            log=LogFactory.getLog(getClass());
    //String          props="UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:"+
    //                  "VIEW_ENFORCER:STATE_TRANSFER:QUEUE";

    final String props=
            "UDP:"
            + "PING(num_initial_members=2;timeout=3000):"
            + "FD:"
            + "pbcast.PBCAST(gossip_interval=5000;gc_lag=50):"
            + "UNICAST:"
            + "FRAG:"
            + "pbcast.GMS:"
            + "pbcast.STATE_TRANSFER";

    private void integrate(Hashtable state) {
        String key;
        if(state == null)
            return;
        for(Enumeration e=state.keys(); e.hasMoreElements();) {
            key=(String)e.nextElement();
            stocks.put(key, state.get(key)); // just overwrite
        }
    }

    public void viewAccepted(View new_view) {
        System.out.println("Accepted view (" + new_view.size() + new_view.getMembers() + ')');
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }

    public void start() {
        try {
            channel=new JChannel(props);
            disp=new RpcDispatcher(channel, this, this, this);
            channel.connect(channel_name);
            System.out.println("\nQuote Server started at " + new Date());
            System.out.println("Joined channel '" + channel_name + "' (" + channel.getView().size() + " members)");
            channel.getState(null, 0);
            System.out.println("Ready to serve requests");
        }
        catch(Exception e) {
            log.error("QuoteServer.start() : " + e);
            System.exit(-1);
        }
    }

    /* Quote methods: */

    public float getQuote(String stock_name) throws Exception {
        System.out.print("Getting quote for " + stock_name + ": ");
        Float retval=(Float)stocks.get(stock_name);
        if(retval == null) {
            System.out.println("not found");
            throw new Exception("Stock " + stock_name + " not found");
        }
        System.out.println(retval.floatValue());
        return retval.floatValue();
    }

    public void setQuote(String stock_name, Float value) {
        System.out.println("Setting quote for " + stock_name + ": " + value);
        stocks.put(stock_name, value);
    }

    public Hashtable getAllStocks() {
        System.out.print("getAllStocks: ");
        printAllStocks();
        return stocks;
    }

    public void printAllStocks() {
        System.out.println(stocks);
    }

    public void receive(Message msg) {
    }

    public byte[] getState() {
        try {
            return Util.objectToByteBuffer(stocks.clone());
        }
        catch(Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public void setState(byte[] state) {
        try {
            integrate((Hashtable)Util.objectFromByteBuffer(state));
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String args[]) {
        try {
            QuoteServer server=new QuoteServer();
            server.start();
            while(true) {
                Util.sleep(10000);
            }
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

}
