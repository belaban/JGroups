// $Id: Hot_Error.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

public class Hot_Error {
    private int val;
    private String msg;

    public Hot_Error(int value, String message) {
	val = value;
	msg = message;
    }

    public String toString() {
	return new String("Hot_Error: " + val + ": " + msg);
    }

}
