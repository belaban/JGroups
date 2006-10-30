package org.jgroups.jmx.protocols;

import java.net.UnknownHostException;

/**
 * @author Bela Ban
 * @version $Id: TCP.java,v 1.4 2006/10/30 20:07:10 bstansberry Exp $
 */
public class TCP extends TP implements TCPMBean {
    org.jgroups.protocols.TCP p;

    public TCP() {
    }

    public TCP(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.TCP)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.TCP)p;
    }


    public int getOpenConnections() {
        return p.getOpenConnections();
    }

    public String getBindAddr() {
        return p.getBindAddress();
    }

    public void setBindAddr(String bind_addr) {
        try {
            p.setBindAddress(bind_addr);
        }
        catch(UnknownHostException e) {
            IllegalArgumentException iae = new IllegalArgumentException("Unknown host " + bind_addr);
            iae.initCause(e);
            throw iae;
        }
    }

    public int getStartPort() {
        return p.getStartPort();
    }

    public void setStartPort(int start_port) {
        p.setStartPort(start_port);
    }

    public int getEndPort() {
        return p.getEndPort();
    }

    public void setEndPort(int end_port) {
        p.setEndPort(end_port);
    }

    public long getReaperInterval() {
        return p.getReaperInterval();
    }

    public void setReaperInterval(long reaper_interval) {
        p.setReaperInterval(reaper_interval);
    }

    public long getConnExpireTime() {
        return p.getConnExpireTime();
    }

    public void setConnExpireTime(long conn_expire_time) {
        p.setConnExpireTime(conn_expire_time);
    }

    public String printConnections() {
        return p.printConnections();
    }
}
