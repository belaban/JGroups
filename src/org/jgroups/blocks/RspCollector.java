// $Id: RspCollector.java,v 1.2 2004/03/30 06:47:12 belaban Exp $

package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;


public interface RspCollector {
    void receiveResponse(Message msg);
    void suspect(Address mbr);
    void viewChange(View new_view);
}
