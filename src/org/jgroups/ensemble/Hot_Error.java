// $Id: Hot_Error.java,v 1.2 2004/07/05 05:48:44 belaban Exp $

package org.jgroups.ensemble;

public class Hot_Error {
    private int val;
    private String msg;

    public Hot_Error(int value, String message) {
	val = value;
	msg = message;
    }

    public String toString() {
	return "Hot_Error: " + val + ": " + msg;
    }

}
