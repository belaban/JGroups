package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.PingHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jgroups.Event.GET_LOGICAL_PHYSICAL_MAPPINGS;

/**
 * Tests https://issues.redhat.com/browse/JGRP-2741 (return_entire_cache=true), reduce response messages
 * @author Bela Ban
 * @since  5.3.2
 */
@Test(groups= Global.STACK_DEPENDENT,singleThreaded=true)
public class DiscoveryTest2 extends ChannelTestBase {
    protected JChannel            a, b, c, d, e;
    protected static final String CLUSTER=DiscoveryTest2.class.getSimpleName();

    @BeforeMethod protected void setup() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        c=createChannel().name("C");
        d=createChannel().name("D");
        e=createChannel().name("E");
        makeUnique(a,b,c,d,e);
        Stream.of(a,b,c,d,e).map(ch -> (Discovery)ch.stack().findProtocol(Discovery.class))
          .forEach(discovery -> discovery.returnEntireCache(true));
        a.connect(CLUSTER);
        b.connect(CLUSTER);
        c.connect(CLUSTER);
        d.connect(CLUSTER);
        // don't connect E yet!
        Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b,c,d);
    }

    @AfterMethod void tearDown() throws Exception {
        Util.close(e,d,c,b,a);
    }


    /** Tests https://issues.redhat.com/browse/JGRP-2741 */
    public void testReturnEntireCache() throws Exception {
        Util.waitUntil(5000, 500, () -> Stream.of(a, b, c, d).allMatch(ch -> {
            var cache=(Map<Address,PhysicalAddress>)ch.down(new Event(GET_LOGICAL_PHYSICAL_MAPPINGS));
            return cache.size() == 4;
        }), () -> Stream.of(a, b, c, d).map(ch -> {
            var cache=(Map<Address,PhysicalAddress>)ch.down(new Event(GET_LOGICAL_PHYSICAL_MAPPINGS));
            return String.format("%s: %s", ch.getAddress(), cache.toString());
        }).collect(Collectors.joining("\n")));

        for(JChannel ch: List.of(a,b,c,d,e)) {
            ch.stack().removeProtocol(MERGE3.class);
            Discovery discovery=ch.stack().findProtocol(Discovery.class);
            discovery.sendCacheOnJoin(false);
        }

        Discovery discovery=e.stack().findProtocol(Discovery.class);
        DiscoveryResponseHandler rh=new DiscoveryResponseHandler().setId(discovery.getId());
        e.stack().insertProtocol(rh, ProtocolStack.Position.BELOW, Discovery.class);
        e.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b,c,d,e);
        Map<Address,Collection<PingData>> map=rh.map();
        assert map.size() == 4 : String.format("cache: %s", map);
        String s=map.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
        System.out.printf("-- cache:\n%s", s);
    }

    protected static class DiscoveryResponseHandler extends Protocol {
        protected final Map<Address,Collection<PingData>> map=new ConcurrentHashMap<>();

        protected Map<Address,Collection<PingData>> map() {return map;}

        @Override
        public Object up(Message msg) {
            PingHeader hdr=msg.getHeader(id);
            if(hdr != null && hdr.type() == PingHeader.GET_MBRS_RSP)
                handlePingResponse(msg);
            return up_prot.up(msg);
        }

        @Override
        public void up(MessageBatch batch) {
            for(Message msg: batch) {
                PingHeader hdr=msg.getHeader(id);
                if(hdr != null && hdr.type() == PingHeader.GET_MBRS_RSP)
                    handlePingResponse(msg);
            }
            up_prot.up(batch);
        }

        protected void handlePingResponse(Message msg) {
            List<PingData> l=readPingData(msg.getArray(), msg.getOffset(), msg.getLength());
            System.out.printf("-- %s: received %d ping responses from %s: %s\n",
                              local_addr, l.size(), msg.src(), l);
            for(PingData data: l) {
                Collection<PingData> list=map.computeIfAbsent(msg.src(), a -> new ConcurrentLinkedQueue<>());
                list.add(data);
            }
        }

        protected List<PingData> readPingData(byte[] buffer, int offset, int length) {
            try {
                if(buffer == null)
                    return null;
                return deserialize(buffer, offset, length);
            }
            catch(Exception ex) {
                log.error("%s: failed reading PingData from message: %s", local_addr, ex);
                return null;
            }
        }

        protected static List<PingData> deserialize(final byte[] data, int offset, int length) throws Exception {
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
            int num=in.readInt();
            if(num == 0)
                return null;
            List<PingData> list=new ArrayList<>(num);
            for(int i=0; i < num; i++) {
                PingData pd=new PingData();
                pd.readFrom(in);
                list.add(pd);
            }
            return list;
        }
    }
}
