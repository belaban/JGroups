package org.jgroups.tests.adaptjms;

import java.io.Serializable;

/**
 * @author Bela Ban Jan 11
 * @author 2004
 * @version $Id: Request.java,v 1.1 2004/02/20 22:54:05 belaban Exp $
 */
public class Request implements Serializable {
    final static int DISCOVERY_REQ=1;
    final static int NEW_MEMBER=2;
    final static int DATA=3;

    int type=0;
    Object arg=null;

    Request(int type, Object arg) {
        this.type=type;
        this.arg=arg;
    }

}
