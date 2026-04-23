package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.util.ByteArrayDataOutputStream;

/**
 * Bundler which doesn't bundle :-) Can be used to measure the diff between bundling and non-bundling (e.g. at runtime).
 * Not really meant for production.
 * @author Bela Ban
 * @since  4.0
 */
public class NoBundler extends BaseBundler {
    public int       size() {return 0;}
    public int       getQueueSize() {
        return -1;
    }


    @Override
    public void send(Message msg) throws Exception {
        boolean stats_enabled=transport.statsEnabled();
        long start=stats_enabled? System.nanoTime() : 0;
        ByteArrayDataOutputStream buffer=new ByteArrayDataOutputStream(msg.length() + 128).growExponentially(true);
        sendSingle(msg.dest(), msg, buffer);
        if(stats_enabled) {
            long time=System.nanoTime() - start;
            avg_send_time.add(time);
        }
    }

}