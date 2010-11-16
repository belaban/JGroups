package org.jgroups.protocols;

import org.jgroups.Address;

import java.util.List;

/**
 * @author Bela Ban
 * @since 2.11
 */
public class JDBC_PING extends FILE_PING {

    public void init() throws Exception {
        super.init();


    }

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
