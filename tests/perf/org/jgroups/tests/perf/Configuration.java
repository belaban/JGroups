package org.jgroups.tests.perf;

/**
 * Captures all config for IPerf
 * @author Bela Ban
 * @version $Id: Configuration.java,v 1.1 2008/07/24 09:22:09 belaban Exp $
 */
public class Configuration {
    private int size=10 * 1000 * 1000;
    private boolean sender=false;
    private String transport="org.jgroups.tests.perf.transport.JGroupsTransport";

    public boolean isSender() {
        return sender;
    }

    public void setSender(boolean sender) {
        this.sender=sender;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size=size;
    }

    public String getTransport() {
        return transport;
    }

    public void setTransport(String transport) {
        this.transport=transport;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("size=" + size).append("\n");
        sb.append("sender=" + sender).append("\n");
        sb.append("transport=" + transport + "\n");
        return sb.toString();
    }
}
