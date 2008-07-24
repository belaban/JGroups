package org.jgroups.tests.perf.transports;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import org.jgroups.tests.perf.Transport;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Transport with a minimal stack (usually only UDP or TCP transport layer !) and explicit notion of
 * cluster membership (listed in properties, config.txt)
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: JGroupsClusterTransport.java,v 1.4 2008/07/24 10:06:00 belaban Exp $
 */
public class JGroupsClusterTransport extends JGroupsTransport implements Transport {
    List members;

    public JGroupsClusterTransport() {
        super();
    }

   

    public void create(Properties properties) throws Exception {
        super.create(properties);
        String cluster_def=config.getProperty("cluster");
        if(cluster_def == null)
            throw new Exception("property 'cluster' is not defined");
        members=parseCommaDelimitedList(cluster_def);
    }


    public void send(Object destination, byte[] payload, boolean oob) throws Exception {
        if(destination != null) {
            Message msg=new Message((Address)destination, null, payload);
            if(oob)
                msg.setFlag(Message.OOB);
            channel.send(msg);
        }
        else {
            // we don't know the membership from discovery, so we need to send individually
            Address mbr;
            for(int i=0; i < members.size(); i++) {
                mbr=(Address)members.get(i);
                Message msg=new Message((Address)destination, null, payload);
                if(oob)
                    msg.setFlag(Message.OOB);
                msg.setDest(mbr);
                channel.send(msg);
            }
        }
    }


    private List parseCommaDelimitedList(String s) throws Exception {
        List retval=new ArrayList();
        StringTokenizer tok;
        String hostname, tmp;
        int    port;
        Address addr;
        int index;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            tmp=tok.nextToken();
            index=tmp.indexOf(':');
            if(index == -1)
                throw new Exception("host must be in format <host:port>, was " + tmp);
            hostname=tmp.substring(0, index);
            port=Integer.parseInt(tmp.substring(index+1));
            addr=new IpAddress(hostname, port);
            retval.add(addr);
        }
        return retval;
    }
}
