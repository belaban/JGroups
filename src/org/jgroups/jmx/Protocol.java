package org.jgroups.jmx;

import org.jgroups.stack.ProtocolObserver;

import java.util.Properties;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: Protocol.java,v 1.7 2005/07/26 11:15:19 belaban Exp $
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

    public String printStats() {
        return prot.printStats();
    }

    public Map dumpStats() {
        return prot.dumpStats();
    }

    public boolean getUpThread() {
        return prot.upThreadEnabled();
    }

    public boolean getDownThread() {
        return prot.downThreadEnabled();
    }

    public void setObserver(ProtocolObserver observer) {
        prot.setObserver(observer);
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
