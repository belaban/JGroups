package org.jgroups.tests.perf;

import java.net.InetAddress;

/**
 * Captures all config for IPerf
 * @author Bela Ban
 * @version $Id: Configuration.java,v 1.4 2008/09/12 15:10:52 belaban Exp $
 */
public class Configuration {
    private int size=10 * 1000 * 1000;
    private long time=0; // time to send
    private int chunk_size=1000; // send in chunks of 1000 bytes
    private boolean sender=false;
    private String transport="org.jgroups.tests.perf.transports.JGroupsTransport";
    private String[] transport_args;
    private boolean   jmx=false;
    private InetAddress bind_addr=null;

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
        this.time=0;
    }

    public int getChunkSize() {
        return chunk_size;
    }

    public void setChunkSize(int chunk_size) {
        this.chunk_size=chunk_size;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time=time;
        this.size=0;
    }

    public String getTransport() {
        return transport;
    }

    public InetAddress getBindAddress() {
        return bind_addr;
    }

    public void setBindAddress(InetAddress bind_addr) {
        this.bind_addr=bind_addr;
    }

    public boolean isJmx() {
        return jmx;
    }

    public void setJmx(boolean jmx) {
        this.jmx=jmx;
    }

    public void setTransport(final String transport) {
        if(transport == null) return;
        if(transport.equalsIgnoreCase("udp"))
            this.transport="org.jgroups.tests.perf.transports.UdpTransport";
        else if(transport.equalsIgnoreCase("tcp"))
            this.transport="org.jgroups.tests.perf.transports.TcpTransport";
        else if(transport.equalsIgnoreCase("jgroups"))
            this.transport="org.jgroups.tests.perf.transports.JGroupsTransport";
        else if(transport.equalsIgnoreCase("jgroupscluster"))
            this.transport="org.jgroups.tests.perf.transports.JGroupsClusterTransport";
        else
            this.transport=transport;
    }

    public String[] getTransportArgs() {
        return transport_args;
    }

    public void setTransportArgs(String[] transport_args) {
        this.transport_args=transport_args;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("size=" + size).append("\n");
        sb.append("sender=" + sender).append("\n");
        sb.append("transport=" + transport + "\n");
        sb.append("jmx=" + jmx + "\n");
        return sb.toString();
    }
}
