package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.Address;

import java.util.Properties;
import java.util.HashMap;

/**
 * Provides various stats
 * @author Bela Ban
 * @version $Id: STATS.java,v 1.1 2005/06/07 09:03:20 belaban Exp $
 */
public class STATS extends Protocol {
    long sent_msgs, sent_bytes, sent_ucasts, sent_mcasts, received_ucasts, received_mcasts;
    long received_msgs, received_bytes, sent_ucast_bytes, sent_mcast_bytes, received_ucast_bytes, received_mcast_bytes;

    /** HashMap<Address,Entry>, maintains stats per target destination */
    HashMap sent=new HashMap();

    /** HashMap<Address,Entry>, maintains stats per receiver */
    HashMap received=new HashMap();

    final short UP=1;
    final short DOWN=2;


    public String getName() {
        return "STATS";
    }

    public boolean setProperties(Properties props) {
        super.setProperties(props);
        down_thread=false; // never use a down thread
        up_thread=false;   // never use an up thread

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }

    public void resetStats() {
        sent_msgs=sent_bytes=sent_ucasts=sent_mcasts=received_ucasts=received_mcasts=0;
        received_msgs=received_bytes=sent_ucast_bytes=sent_mcast_bytes=received_ucast_bytes=received_mcast_bytes=0;
        sent.clear();
        received.clear();
    }


    public long getSentMessages() {return sent_msgs;}
    public long getSentBytes() {return sent_bytes;}
    public long getSentUnicastMessages() {return sent_ucasts;}
    public long getSentUnicastBytes() {return sent_ucast_bytes;}
    public long getSentMcastMessages() {return sent_mcasts;}
    public long getSentMcastBytes() {return sent_mcast_bytes;}

    public long getReceivedMessages() {return received_msgs;}
    public long getReceivedBytes() {return received_bytes;}
    public long getReceivedUnicastMessages() {return received_ucasts;}
    public long getReceivedUnicastBytes() {return received_ucast_bytes;}
    public long getReceivedMcastMessages() {return received_mcasts;}
    public long getReceivedMcastBytes() {return received_mcast_bytes;}


    public void up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            updateStats(msg, UP);
        }
        passUp(evt);
    }



    public void down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            updateStats(msg, DOWN);
        }
        passDown(evt);
    }


    public String printStats() {
        return null;
    }

    private void updateStats(Message msg, short direction) {
        int     length;
        HashMap map;
        boolean mcast;
        Address dest, src;

        if(msg == null) return;
        length=msg.getLength();
        dest=msg.getDest();
        src=msg.getSrc();
        mcast=dest == null || dest.isMulticastAddress();

        if(direction == UP) { // received
            received_msgs++;
            received_bytes+=length;
            if(mcast) {
                received_mcasts++;
                received_mcast_bytes+=length;
            }
            else {
                received_ucasts++;
                received_ucast_bytes+=length;
            }
        }
        else {                // sent
            sent_msgs++;
            sent_bytes+=length;
            if(mcast) {
                sent_mcasts++;
                sent_mcast_bytes+=length;
            }
            else {
                sent_ucasts++;
                sent_ucast_bytes+=length;
            }
        }

        Address key=direction == UP? dest : src;
        map=direction == UP? received : sent;
        Entry entry=(Entry)map.get(key);
        if(entry == null) {
            entry=new Entry();
            map.put(key, entry);
        }

        if(direction == UP) { // received
            entry.received_msgs++;
            entry.received_bytes+=length;
            if(mcast) {
                entry.received_mcasts++;
                entry.received_mcast_bytes+=length;
            }
            else {
                entry.received_ucasts++;
                entry.received_ucast_bytes+=length;
            }
        }
        else {                // sent
            entry.sent_msgs++;
            entry.sent_bytes+=length;
            if(mcast) {
                entry.sent_mcasts++;
                entry.sent_mcast_bytes+=length;
            }
            else {
                entry.sent_ucasts++;
                entry.sent_ucast_bytes+=length;
            }
        }
    }




    static class Entry {
        long sent_msgs, sent_bytes, sent_ucasts, sent_mcasts, received_ucasts, received_mcasts;
        long received_msgs, received_bytes, sent_ucast_bytes, sent_mcast_bytes, received_ucast_bytes, received_mcast_bytes;
    }



}
