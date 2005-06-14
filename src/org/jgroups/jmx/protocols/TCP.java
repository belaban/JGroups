package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

import java.net.InetAddress;

/**
 * @author Bela Ban
 * @version $Id: TCP.java,v 1.1 2005/06/14 09:51:08 belaban Exp $
 */
public class TCP extends Protocol implements TCPMBean {
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

    public long getNumMessagesSent() {
        return p.getNumMessagesSent();
    }

    public long getNumMessagesReceived() {
        return p.getNumMessagesReceived();
    }

    public int getOpenConnections() {
        return p.getOpenConnections();
    }

    public InetAddress getBindAddr() {
        return p.getBindAddr();
    }

    public void setBindAddr(InetAddress bind_addr) {
        p.setBindAddr(bind_addr);
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

    public boolean isLoopback() {
        return p.isLoopback();
    }

    public void setLoopback(boolean loopback) {
        p.setLoopback(loopback);
    }

    public String printConnections() {
        return p.printConnections();
    }
}
