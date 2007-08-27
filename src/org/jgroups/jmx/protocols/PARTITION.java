package org.jgroups.jmx.protocols;

/**
 * @author Bela Ban
 * @version $Id: PARTITION.java,v 1.1 2007/08/27 10:28:48 belaban Exp $
 */
public class PARTITION extends PING implements PARTITIONMBean {
    org.jgroups.protocols.PARTITION partiton;

    public PARTITION() {
    }

    public PARTITION(org.jgroups.stack.Protocol p) {
        super(p);
        this.partiton=(org.jgroups.protocols.PARTITION)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.partiton=(org.jgroups.protocols.PARTITION)p;
    }


    public boolean isPartitionOn() {
        return partiton.isPartitionOn();
    }

    public void startPartition() {
        partiton.startPartition();
    }

    public void stopPartition() {
        partiton.stopPartition();
    }
}