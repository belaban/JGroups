// $Id: DrawCommand.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.demos;

import java.io.Serializable;


public class DrawCommand implements Serializable {
    static final int DRAW=1;
    static final int CLEAR=2;
    int mode;
    int x=0;
    int y=0;
    int r=0;
    int g=0;
    int b=0;

	
    DrawCommand(int mode) {
	this.mode=mode;
    }
	
    DrawCommand(int mode, int x, int y, int r, int g, int b) {
	this.mode=mode;
	this.x=x;
	this.y=y;
	this.r=r;
	this.g=g;
	this.b=b;
    }


    DrawCommand Copy() {
	return new DrawCommand(mode, x, y, r, g, b);
    }
	
	
    public String toString() {
	StringBuffer ret=new StringBuffer();
	switch(mode) {
	case DRAW: ret.append("DRAW(" + x + ", " + y + ") [" + r + "|" + g + "|" + b + "]");
	    break;
	case CLEAR: ret.append("CLEAR");
	    break;
	default:
	    return "<undefined>";
	}
	return ret.toString();
    }
	
}
