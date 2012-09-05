
package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.View;


public interface RspCollector {
    void receiveResponse(Object response_value, Address sender, boolean is_exception);
    void suspect(Address mbr);
    void viewChange(View new_view);
    void siteUnreachable(short site);
    void transportClosed();
}
