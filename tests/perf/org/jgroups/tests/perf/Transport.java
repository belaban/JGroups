package org.jgroups.tests.perf;

import java.util.Properties;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: Transport.java,v 1.1 2004/01/23 00:08:31 belaban Exp $
 */
public interface Transport {
    void create(Properties properties) throws Exception;
    Object getLocalAddress();
    void start() throws Exception;
    void stop();
    void destroy();
    void setReceiver(Receiver r);
    void send(Object destination, byte[] payload) throws Exception;
}
