package org.jgroups.tests;

import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;

/**
 * Tests UnicastTest with SHARED_LOOPBACK and 2 UnicastTest instances
 * @author Bela Ban
 * @since  3.5
 */
public class UnicastTestSharedLoopback {
    public static void main(String[] args) throws Exception {



        UnicastTest a=new UnicastTest();
        a.init(props(), 0, "A");

        UnicastTest b=new UnicastTest();
        b.init(props(), 0, "B");


        a.eventLoop();
    }


    protected static Protocol[] props() {
        return new Protocol[]{
          new SHARED_LOOPBACK().setValue("bundler_type", "sender-sends").setValue("ignore_dont_bundle", false),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new UNICAST3().setValue("conn_expiry_timeout", 0).setValue("conn_close_timeout", 0),
          new STABLE(),
          new GMS()};
    }
}
