package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: Protocol.java,v 1.1 2005/06/03 08:49:17 belaban Exp $
 */
public class Protocol implements ProtocolMBean {
    org.jgroups.stack.Protocol prot;

    public Protocol() {

    }

    public Protocol(org.jgroups.stack.Protocol p) {
        this.prot=p;
    }

    public String getName() {
        return prot.getName();
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        this.prot=p;
    }

    public String getPropertiesAsString() {
        return prot.getProperties().toString();
//        Properties p=prot.getProperties();
//        StringBuffer sb=new StringBuffer();
//        Map.Entry entry;
//        for(Iterator it=p.entrySet().iterator(); it.hasNext();) {
//            entry=(Map.Entry)it.next();
//            sb.append(entry.getKey()).append("=").append(entry.getValue()).append('\n');
//        }
//        return sb.toString();
    }

    public void setProperties(Properties p) {
        prot.setProperties(p);
    }

    public boolean getUpThread() {
        return prot.upThreadEnabled();
    }

    public boolean getDownThread() {
        return prot.downThreadEnabled();
    }

    public int getUpEvents() {
        return prot.getUpQueue().size();
    }

    public int getDownEvents() {
        return prot.getDownQueue().size();
    }

    public String dumpUpQueue() {
        return Util.dumpQueue(prot.getUpQueue());
    }

    public String dumpDownQueue() {
        return Util.dumpQueue(prot.getDownQueue());
    }

    public void setObserver(ProtocolObserver observer) {
        prot.setObserver(observer);
    }

    public String requiredUpServices() {
        return prot.requiredUpServices().toString();
    }

    public String requiredDownServices() {
        return prot.requiredDownServices().toString();
    }

    public String providedUpServices() {
        return prot.providedUpServices().toString();
    }

    public String providedDownServices() {
        return prot.providedDownServices().toString();
    }

    public void create() throws Exception {
        prot.init();
    }

    public void start() throws Exception {
        prot.start();
    }

    public void stop() {
        prot.stop();
    }

    public void destroy() {
        prot.destroy();
    }
}
