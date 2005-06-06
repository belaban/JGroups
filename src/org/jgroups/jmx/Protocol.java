package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;

/**
 * @author Bela Ban
 * @version $Id: Protocol.java,v 1.2 2005/06/06 15:33:59 belaban Exp $
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

    public boolean getStatsEnabled() {
        return prot.statsEnabled();
    }

    public void setStatsEnabled(boolean flag) {
        prot.enableStats(flag);
    }

    public void resetStats() {
        prot.resetStats();
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
        Vector ret=prot.requiredUpServices();
        return ret != null? prot.requiredUpServices().toString() : "<empty>";
    }

    public String requiredDownServices() {
        Vector ret=prot.requiredDownServices();
        return ret != null? prot.requiredDownServices().toString() : "<empty>";
    }

    public String providedUpServices() {
        Vector ret=prot.providedUpServices();
        return ret != null? prot.providedUpServices().toString() : "<empty>";
    }

    public String providedDownServices() {
        Vector ret=prot.providedDownServices();
        return ret != null? prot.providedDownServices().toString() : "<empty>";
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
