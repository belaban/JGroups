package org.jgroups;

import org.jgroups.util.Util;

import java.io.InputStream;
import java.io.OutputStream;

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

    public void getState(OutputStream ostream) {
        Util.close(ostream);
    }

    public void setState(InputStream istream) {
        Util.close(istream);
    }

    public void viewAccepted(View view) {
    }

    public void suspect(Address mbr) {
    }

    public void block() {
    }

    public void unblock() {
    }
}
