// $Id: RspCollector.java,v 1.4 2007/02/16 09:06:57 belaban Exp $

package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;


public interface RspCollector {
    void receiveResponse(Object response_value, Address sender);
    void suspect(Address mbr);
    void viewChange(View new_view);
}
