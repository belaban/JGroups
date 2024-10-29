package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.NoBundler;
import org.jgroups.protocols.TP;

/**
 * A dummy implementation of {@link TP}
 * @author Bela Ban
 * @since  5.4
 */
public class MockTransport extends TP {

    public void               init() throws Exception {
        super.init();
        setBundler(new NoBundler());
    }
    public boolean            supportsMulticasting() {return true;}
    public void               sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
    public String             getInfo()              {return null;}
    protected PhysicalAddress getPhysicalAddress()   {return null;}
    public MockTransport      cluster(AsciiString s) {this.cluster_name=s; return this;}


    public Object down(Message msg) {
        return null;
    }


}
