package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Bela Ban
 */
public class ReceiverAdapter implements Receiver {

    public void receive(Message msg) {
    }

    public void getState(OutputStream output) throws Exception {
    }

    public void setState(InputStream input) throws Exception {
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
