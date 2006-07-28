package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Bela Ban
 * @version $Id: ExtendedReceiverAdapter.java,v 1.3 2006/07/28 07:19:50 belaban Exp $
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

	public void getState(OutputStream ostream) {

	}

	public void getState(String state_id, OutputStream ostream) {
	}

	public void setState(InputStream istream) {
	}

	public void setState(String state_id, InputStream istream) {
	}
}
