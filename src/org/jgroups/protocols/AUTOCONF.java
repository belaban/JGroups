// $Id: AUTOCONF.java,v 1.20 2007/08/27 08:09:19 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Senses the network configuration when it is initialized (in init()) and sends a CONFIG event up
 * and down the stack. The CONFIG event contains a hashmap, with strings as keys (e.g. "frag_size")
 * and Objects as values. Certain protocols can set some of their properties when receiving the CONFIG
 * event.
 * <p>
 * This protocol should be placed above the transport protocol (e.g. UDP). It is not needed for TCP.
 * <p>
 * Example: senses the network send and receive buffers, plus the max size of a message to be sent and
 * generates a CONFIG event containing "frag_size", "send_buf_size" and "receive_buf_size" keys.
 * 
 * @author Bela Ban
 */
public class AUTOCONF extends Protocol {
    final Map<String,Object> config=new HashMap<String,Object>();
    static int num_iterations=10; // to find optimal frag_size

    /** Number of bytes to subtract from computed fragmentation size, due to (a) headers and
     * (b) serialization overhead */
    static int frag_overhead=1000;


    public String getName() {
        return "AUTOCONF";
    }


    public void init() throws Exception {
        senseNetworkConfiguration();
        if(log.isDebugEnabled()) log.debug("configuration is\n" + config);
    }

    public void start() throws Exception {
        if(config != null && config.size() > 0) {
            Event config_evt=new Event(Event.CONFIG, config);
            down_prot.down(config_evt);
            up_prot.up(config_evt);
        }
    }


    /**
     * Setup the Protocol instance acording to the configuration string
     */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("num_iterations");
        if(str != null) {
            num_iterations=Integer.parseInt(str);
            props.remove("num_iterations");
        }

        str=props.getProperty("frag_overhead");
        if(str != null) {
            frag_overhead=Integer.parseInt(str);
            props.remove("frag_overhead");
        }

        if(props.size() > 0) {
            log.error("AUTOCONF.setProperties(): the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }




    /* -------------------------------------- Private metods ------------------------------------------- */
    void senseNetworkConfiguration() {
        int max_frag_size=senseMaxFragSize();
        if(max_frag_size <= 0) {
            if(log.isErrorEnabled()) log.error("max_frag_size is invalid: " + max_frag_size);
        }
        else
            config.put("frag_size", new Integer(max_frag_size));
        senseMaxSendBufferSize(config);
        senseMaxReceiveBufferSize(config);
    }

    public static int senseMaxFragSizeStatic() {
        return new AUTOCONF().senseMaxFragSize();
    }

    /**
     * Tries to find out the max number of bytes in a DatagramPacket we can send by sending increasingly
     * larger packets, until there is an exception (e.g., java.io.IOException: message too long).
     */
    public int senseMaxFragSize() {
        int max_send=32000;
        int upper;
        int lower=0;
        int highest_failed=-1;
        DatagramSocket sock;
        byte[] buf;
        DatagramPacket packet;
        InetAddress local_addr;


        try {
            sock=new DatagramSocket();
            local_addr=InetAddress.getLocalHost();
        }
        catch(Exception ex) {
            if(log.isWarnEnabled()) log.warn("failed creating DatagramSocket: " + ex);
            return 0;
        }

        try {
            upper=max_send;
            for(int i=0; i < num_iterations && lower < upper; i++) { // iterations to approximate frag_size
                try {
                    buf=new byte[upper];
                    // System.out.println("** upper=" + upper + " (lower=" + lower + ")");
                    packet=new DatagramPacket(buf, buf.length, local_addr, 9);
                    sock.send(packet);
                    lower=Math.max(lower, upper);
                    upper=upper * 2;
                    if(highest_failed > -1)
                        upper=Math.min(highest_failed, upper);
                }
                catch(IOException io_ex) {
                    if(highest_failed > -1)
                        highest_failed=Math.min(highest_failed, upper); // never exceed max_upper
                    else
                        highest_failed=upper;
                    upper=(upper + lower) / 2;
                }
                catch(Throwable ex) {
                    if(log.isWarnEnabled()) log.warn("exception=" + ex);
                    break;
                }
            }

            /** Reduce the frag_size a bit to prevent packets that are too large (see bug #854887) */
            lower-=frag_overhead;
            if(log.isDebugEnabled()) log.debug("frag_size=" + lower);
            return lower;
        }
        finally {
            if(sock != null)
                sock.close();
        }
    }


    void senseMaxSendBufferSize(Map<String,Object> map) {
        DatagramSocket sock;
        int max_size=4096, retval=max_size;

        if(map != null && map.containsKey("frag_size)"))
            max_size=((Integer)map.get("frag_size")).intValue();

        try {
            sock=new DatagramSocket();
            while(max_size < 1000000) {
                sock.setSendBufferSize(max_size);
                if((retval=sock.getSendBufferSize()) < max_size)
                    return;
                max_size*=2;
            }
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("failed getting the max send buffer size: " + ex +
                    ". Defaulting to " + retval);
        }
        finally {
            map.put("send_buf_size", new Integer(retval));
        }
    }



    void senseMaxReceiveBufferSize(Map<String,Object> map) {
        DatagramSocket sock;
        int max_size=4096, retval=max_size;

        try {
            sock=new DatagramSocket();
            while(max_size < 1000000) {
                sock.setReceiveBufferSize(max_size);
                if((retval=sock.getReceiveBufferSize()) < max_size)
                    return;
                max_size*=2;
            }
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("failed getting the max send buffer size: " + ex +
                    ". Defaulting to " + retval);
        }
        finally {
            map.put("recv_buf_size", new Integer(retval));
        }
    }


    /* ----------------------------------- End of Private metods --------------------------------------- */

    public static void main(String[] args) {
        int frag_size=new AUTOCONF().senseMaxFragSize();
        System.out.println("frag_size: " + frag_size);
    }

}
