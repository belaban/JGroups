package org.jgroups;

/**
 * @author Bela Ban
 * @version $Id: ExtendedReceiverAdapter.java,v 1.1 2006/03/16 09:56:28 belaban Exp $
 */
public class ExtendedReceiverAdapter implements ExtendedReceiver {
    public byte[] getState(String state_id) {
        return null;
    }

    public void setState(String state_id, byte[] state) {
    }

    public void receive(Message msg) {
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    public void viewAccepted(View new_view) {
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }
}
