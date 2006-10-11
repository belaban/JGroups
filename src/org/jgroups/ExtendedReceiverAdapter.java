package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: ExtendedReceiverAdapter.java,v 1.6 2006/10/11 14:34:36 belaban Exp $
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

    public void unblock() {
    }

    public void getState(OutputStream ostream) {
       Util.close(ostream);
	}

	public void getState(String state_id, OutputStream ostream) {
       Util.close(ostream);
	}

	public void setState(InputStream istream) {
       Util.close(istream);
	}

	public void setState(String state_id, InputStream istream) {
       Util.close(istream);
	}
}
