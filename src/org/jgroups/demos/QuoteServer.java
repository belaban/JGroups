
package org.jgroups.demos;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;




/**
 * Example of a replicated quote server. The server maintains state which consists of a list
 * of quotes and their corresponding values. When it is started, it tries to reach other
 * quote servers to get its initial state. If it does not receive any response after 5
 * seconds, it assumes it is the first server and starts processing requests.<p>
 * Any updates are multicast across the cluster
 * @author Bela Ban
 */

public class QuoteServer extends ReceiverAdapter {
    final Hashtable stocks=new Hashtable();
    Channel channel;
    RpcDispatcher disp;
    static final String channel_name="Quotes";
    final int num_members=1;
    Log            log=LogFactory.getLog(getClass());

    final String props=null; // default stack from JChannel

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

    public void getState(OutputStream ostream) throws Exception {
        Util.objectToStream(stocks, new DataOutputStream(ostream));
    }

    public void setState(InputStream istream) throws Exception {
        integrate((Hashtable)Util.objectFromStream(new DataInputStream(istream)));
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
