package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests https://issues.jboss.org/browse/JGRP-1429
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups=Global.TIME_SENSITIVE,singleThreaded=true)
public class VERIFY_SUSPECT_Test {
    static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");
    long start;

    public void testTimer() {
        VERIFY_SUSPECT ver=new VERIFY_SUSPECT();
        ProtImpl impl=new ProtImpl();
        ver.setUpProtocol(impl);
        ver.setDownProtocol(new NoopProtocol());

        start=System.currentTimeMillis();
        ver.up(new Event(Event.SUSPECT, a));

        Util.sleep(100);
        ver.up(new Event(Event.SUSPECT,b));

        Map<Address,Long> map=impl.getMap();

        for(int i=0; i < 20; i++) {
            if(map.size() == 2)
                break;
            Util.sleep(1000);
        }
        System.out.println("map = " + map);

        long timeout_a=map.get(a), timeout_b=map.get(b);
        assert timeout_a >= 2000 && timeout_a < 2500;
        assert timeout_b >= 2100 && timeout_b < 2500;
    }

     public void testTimer2() {
        VERIFY_SUSPECT ver=new VERIFY_SUSPECT();
        ProtImpl impl=new ProtImpl();
        ver.setUpProtocol(impl);
        ver.setDownProtocol(new NoopProtocol());

        start=System.currentTimeMillis();
        ver.up(new Event(Event.SUSPECT, a));

        Util.sleep(100);
        ver.up(new Event(Event.SUSPECT,b));

        Map<Address,Long> map=impl.getMap();

        for(int i=0; i < 5; i++) {
            Address addr=Util.createRandomAddress(String.valueOf(i));
            ver.up(new Event(Event.SUSPECT, addr));
            Util.sleep(500);
        }

         Util.sleep(3000);
         assert map.size() == 7;
    }

    public void testUnsuspect() {
        VERIFY_SUSPECT ver=new VERIFY_SUSPECT();
        ProtImpl impl=new ProtImpl();
        ver.setUpProtocol(impl);
        ver.setDownProtocol(new Protocol() {
            public Object down(Event evt) {
                return null;
            }
            public Object down(Message msg) {return null;}

            public ThreadFactory getThreadFactory() {
                return new DefaultThreadFactory("foo",false,true);
            }
        });

        start=System.currentTimeMillis();
        ver.up(new Event(Event.SUSPECT, a));
        ver.up(new Event(Event.SUSPECT,b));

        Util.sleep(1000);
        ver.unsuspect(a);

        Util.sleep(3000);
        Map<Address,Long> map=impl.getMap();
        assert map.size() == 1 && map.containsKey(b);
    }


    protected class ProtImpl extends Protocol {
        protected final Map<Address,Long> map=new HashMap<>();

        public Map<Address,Long> getMap() {
            return map;
        }

        public Object up(Event evt) {
            if(evt.getType() == Event.SUSPECT) {
                Address suspect=evt.getArg();
                long diff=System.currentTimeMillis() - start;
                map.put(suspect, diff);
                System.out.println("[" + diff + "] evt = " + evt);
            }
            return null;
        }
    }

    protected static class NoopProtocol extends Protocol {
        public Object down(Event evt) {return null;}
        public Object down(Message msg) {return null;}
        public ThreadFactory getThreadFactory() {return new DefaultThreadFactory("y", false, true);}
    }
}
