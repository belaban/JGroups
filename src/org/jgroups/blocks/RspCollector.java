// $Id: RspCollector.java,v 1.3 2006/08/28 06:51:53 belaban Exp $

package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;


public interface RspCollector {
    void receiveResponse(Object response_value, Address sende);
    void suspect(Address mbr);
    void viewChange(View new_view);
}
