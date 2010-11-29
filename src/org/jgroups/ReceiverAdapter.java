package org.jgroups;

/**
 * @author Bela Ban
 */
public class ReceiverAdapter implements Receiver {

    public void receive(Message msg) {
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    public void viewAccepted(View view) {
    }

    public void suspect(Address mbr) {
    }

    public void block() {
    }
}
