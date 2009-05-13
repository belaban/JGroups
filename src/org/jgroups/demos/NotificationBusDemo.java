// $Id: NotificationBusDemo.java,v 1.9 2009/05/13 13:07:01 belaban Exp $


package org.jgroups.demos;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.NotificationBus;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Vector;


/**
 * Demoes the NotificationBus (without caching). Start a number of members and type in messages. All members will
 * receive the messages. View changes will also be displayed (e.g. member joined, left).
 * @author Bela Ban
 */
public class NotificationBusDemo implements NotificationBus.Consumer {
    NotificationBus bus=null;
    BufferedReader in=null;
    String line;
    final long timeout=0;
    final Vector cache=null;
    Log log=LogFactory.getLog(getClass());


    public void start(String bus_name, String props) {
        try {

            bus=new NotificationBus(bus_name, props);
            bus.setConsumer(this);
            bus.start();
            //System.out.println("Getting the cache from coordinator:");
            //cache=(Vector)bus.getCacheFromCoordinator(3000, 3);
            //if(cache == null) cache=new Vector();
            //System.out.println("cache is " + cache);

            in=new BufferedReader(new InputStreamReader(System.in));
            while(true) {
                try {
                    System.out.print("> ");
                    System.out.flush();
                    line=in.readLine();
                    if(line.startsWith("quit") || line.startsWith("exit")) {
                        bus.stop();
                        bus=null;
                        break;
                    }
                    bus.sendNotification(line);
                }
                catch(Exception e) {
                    log.error(e.toString());
                }
            }
        }
        catch(Exception ex) {
            log.error(ex.toString());
        }
        finally {
            if(bus != null)
                bus.stop();
        }
    }


    public void handleNotification(Serializable n) {
        System.out.println("** Received notification: " + n);
        //if(cache != null)
        //  cache.addElement(n);
        //System.out.println("cache is " + cache);
    }


    public Serializable getCache() {
        // return cache;
        return null;
    }


    public void memberJoined(Address mbr) {
        System.out.println("** Member joined: " + mbr);
    }

    public void memberLeft(Address mbr) {
        System.out.println("** Member left: " + mbr);
    }


    public static void main(String[] args) {
        String name="BusDemo";
        String props="udp.xml";

        for(int i=0; i < args.length; i++) {
            if("-bus_name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            System.out.println("NotificationBusDemo [-help] [-bus_name <name>] " +
                    "[-props <properties>]");
            return;
        }
        System.out.println("Starting NotificationBus with name " + name);
        new NotificationBusDemo().start(name, props);
    }


}
