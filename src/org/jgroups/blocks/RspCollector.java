// $Id: RspCollector.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.blocks;

import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Address;


public interface RspCollector {
    void receiveResponse(Message msg);
    void suspect(Address mbr);
    void viewChange(View new_view);
}
