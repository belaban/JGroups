// $Id: Hot_Buffer.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

public class Hot_Buffer {
    byte[] theBytes;

    public Hot_Buffer() {
	theBytes = null;
    }

    public void toAsciiString(String[] s) {
	try {
	    s[0] = new String(theBytes);
	} catch (Exception e) {}
    }

    public void setBytes(byte[] b) {
	try {
	    theBytes = new byte[b.length];
	    System.arraycopy(b, 0, theBytes, 0, b.length);
	} catch (NullPointerException e) {
            theBytes = null;
	}
    }

    public byte[] getBytes() {
	try {
	    byte[] pBytes = new byte[theBytes.length];
	    System.arraycopy(theBytes, 0, pBytes, 0, theBytes.length);
	    return pBytes;
	} catch (NullPointerException e) {
	    return null;
	}
    }

    public int getLength() {
	try {
	    return theBytes.length;
	}
	catch (NullPointerException e) {
	    return 0;
	}
    }

}
