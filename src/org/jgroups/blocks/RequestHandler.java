// $Id: RequestHandler.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.blocks;


import org.jgroups.Message;


public interface RequestHandler {
    Object handle(Message msg);
}
