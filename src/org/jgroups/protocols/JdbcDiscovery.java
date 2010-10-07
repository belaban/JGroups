package org.jgroups.protocols;

import org.jgroups.Address;

import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: JdbcDiscovery.java,v 1.2 2010/10/07 18:33:33 belaban Exp $
 */
public class JdbcDiscovery extends FILE_PING {


    protected void remove(String clustername, Address addr) {
        super.remove(clustername, addr);
    }

    @Override
    protected List<PingData> readAll(String clustername) {
        return super.readAll(clustername);
    }

    protected void writeToFile(PingData data, String clustername) {
        super.writeToFile(data, clustername);
    }
}
