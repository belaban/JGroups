 package org.jgroups.tests;

 import org.jgroups.*;
 import org.jgroups.logging.Log;
 import org.jgroups.logging.LogFactory;
 import org.jgroups.protocols.*;
 import org.jgroups.protocols.pbcast.GMS;
 import org.jgroups.protocols.pbcast.NAKACK2;
 import org.jgroups.protocols.relay.*;
 import org.jgroups.protocols.relay.config.RelayConfig;
 import org.jgroups.stack.Protocol;
 import org.jgroups.util.Bits;
 import org.jgroups.util.MyReceiver;
 import org.jgroups.util.SizeStreamable;
 import org.jgroups.util.Util;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;
 import java.net.InetAddress;
 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.List;
 import java.util.stream.Stream;

/**
 * Common functionality for relay tests
 * @author Bela Ban
 * @since  5.2.17
 */
public class RelayTests {
    protected final Log                log=LogFactory.getLog(getClass());
    protected static final InetAddress LOOPBACK;

    static {
        LOOPBACK=InetAddress.getLoopbackAddress();
    }


    protected static Protocol[] defaultStack(Protocol ... additional_protocols) {
        Protocol[] protocols={
          new TCP().setBindAddress(LOOPBACK),
          new LOCAL_PING(),
          new MERGE3().setMaxInterval(3000).setMinInterval(1000),
          new NAKACK2().useMcastXmit(false),
          new UNICAST3(),
          new GMS().printLocalAddress(false)
        };
        if(additional_protocols == null)
            return protocols;
        Protocol[] tmp=Arrays.copyOf(protocols, protocols.length + additional_protocols.length);
        System.arraycopy(additional_protocols, 0, tmp, protocols.length, additional_protocols.length);
        return tmp;
    }

    /**
     * Creates a node in a local site that connects to all given sites
     * @param site The local site
     * @param name The name of the node
     * @param bridge The name of the bridge cluster
     * @param sites The sites to which this site connects
     */
    protected static JChannel createNode(Class<? extends RELAY> cl, String site, String name, String bridge,
                                         String ... sites) throws Exception {
        return createNode(cl, site, name, bridge, true, sites);
    }

    protected static JChannel createNode(Class<? extends RELAY> cl, String site, String name, String bridge,
                                         boolean connect, String ... sites) throws Exception {
        RELAY relay=createSymmetricRELAY(cl, site, bridge, sites);
        JChannel ch=new JChannel(defaultStack(relay)).name(name);
        if(connect)
            ch.connect(site);
        return ch;
    }


    protected static RELAY createSymmetricRELAY(Class<? extends RELAY> cl, String local_site, String bridge, String ... sites)
      throws Exception {
        RELAY relay=cl.getDeclaredConstructor().newInstance();
        relay.site(local_site).asyncRelayCreation(false);
        for(String site: sites) {
            RelayConfig.SiteConfig cfg=new RelayConfig.SiteConfig(site)
              .addBridge(new RelayConfig.ProgrammaticBridgeConfig(bridge, defaultStack()));
            relay.addSite(site, cfg);
        }
        return relay;
    }

    protected static void waitUntilRoute(String site_name, boolean present,
                                         long timeout, long interval, JChannel ch) throws Exception {
        RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
        if(relay == null)
            throw new IllegalArgumentException("protocol RELAY not found");

        Util.waitUntil(timeout, interval, () -> {
            Route route=relay.getRoute(site_name);
             return ((route != null && present) || (route == null && !present));
        });
    }

    protected static Route getRoute(JChannel ch, String site_name) {
        RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
        return relay.getRoute(site_name);
    }

    protected static boolean isSiteMaster(JChannel ch) {
        RELAY r=ch.getProtocolStack().findProtocol(RELAY.class);
        return r != null && r.isSiteMaster();
    }

    /** Creates a singleton view for each channel listed and injects it */
    protected static void injectSingletonPartitions(JChannel ... channels) {
        for(JChannel ch: channels) {
            View view=View.create(ch.getAddress(), ch.getView().getViewId().getId()+1, ch.getAddress());
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

    protected static void waitForBridgeView(int expected_size, long timeout, long interval, String cluster,
                                            JChannel... channels) {
        Util.waitUntilTrue(timeout, interval, () -> Stream.of(channels)
          .map(ch -> (RELAY)ch.getProtocolStack().findProtocol(RELAY.class))
          .map(r -> r.getBridgeView(cluster)).allMatch(v -> v != null && v.size() == expected_size));

        System.out.println("Bridge views:\n");
        for(JChannel ch: channels) {
            RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
            View bridge_view=relay.getBridgeView(cluster);
            System.out.println(ch.getAddress() + ": " + bridge_view);
        }

        for(JChannel ch: channels) {
            RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
            View bridge_view=relay.getBridgeView(cluster);
            assert bridge_view != null && bridge_view.size() == expected_size
              : ch.getAddress() + ": bridge view=" + bridge_view + ", expected=" + expected_size;
        }
    }

    protected static class MyRouteStatusListener implements RouteStatusListener {
        protected final Address      local_addr;
        protected final List<String> up=new ArrayList<>(), down=new ArrayList<>();
        protected boolean            verbose;

        protected MyRouteStatusListener(Address local_addr) {
            this.local_addr=local_addr;
        }

        protected List<String>          up()               {return up;}
        protected List<String>          down()             {return down;}
        protected MyRouteStatusListener verbose(boolean b) {this.verbose=b; return this;}
        protected boolean               verbose()          {return verbose;}

        @Override public synchronized void sitesUp(String... sites) {
            if(verbose)
                System.out.printf("%s: UP(%s)\n", local_addr, Arrays.toString(sites));
            up.addAll(Arrays.asList(sites));
        }

        @Override public synchronized void sitesDown(String... sites) {
            if(verbose)
                System.out.printf("%s: DOWN(%s)\n", local_addr, Arrays.toString(sites));
            down.addAll(Arrays.asList(sites));
        }

        protected synchronized MyRouteStatusListener clear() {up.clear(); down.clear(); return this;}

        @Override
        public String toString() {
            return String.format("down: %s, up: %s", down, up);
        }
    }

    protected static class ResponseSender<T> extends MyReceiver<T> {
        protected final JChannel ch;

        public ResponseSender(JChannel ch) {
            this.ch=ch;
        }

        @Override
        public void receive(Message msg) {
            super.receive(msg);
            if(msg.dest() == null || msg.dest() instanceof SiteMaster) { // send unicast response back to sender
                Message rsp=new ObjectMessage(msg.src(),"rsp-" + ch.getAddress());
                if(msg.isFlagSet(Message.Flag.NO_RELAY))
                    rsp.setFlag(Message.Flag.NO_RELAY);
                try {
                    ch.send(rsp);
                }
                catch(Exception e) {
                    System.out.printf("%s: failed sending response: %s", ch.getAddress(), e);
                }
            }
        }
    }

    protected static class UnicastResponseSender<T> extends MyReceiver<T> {
        protected final JChannel ch;

        public UnicastResponseSender(JChannel ch) {
            this.ch=ch;
        }

        public void receive(Message msg) {
            super.receive(msg);
            Object obj=msg.getObject();
            Data data=(Data)obj;
            if(data.type == Data.Type.REQ) {
                Message rsp=new ObjectMessage(msg.src(), new Data(Data.Type.RSP,String.valueOf(ch.getAddress())));
                if(msg.isFlagSet(Message.Flag.NO_RELAY))
                    rsp.setFlag(Message.Flag.NO_RELAY);
                try {
                    ch.send(rsp);
                }
                catch(Exception e) {
                    System.out.printf("%s: failed sending response: %s",ch.getAddress(),e);
                }
            }
        }
    }

    protected static class SiteMasterPickerImpl implements SiteMasterPicker {
        public SiteMasterPickerImpl() {
        }

        public Address pickSiteMaster(List<Address> site_masters, Address original_sender) {
            return site_masters.get(0);
        }

        public Route pickRoute(String site, List<Route> routes, Address original_sender) {
            return routes.get(0);
        }
    }

    protected static class Data implements SizeStreamable {
        enum Type {REQ,RSP}
        protected Type   type;
        protected String payload;

        public Data() {}
        public Data(Type t, String s) {
            type=t;
            payload=s;
        }

        public Type   type()    {return type;}
        public String payload() {return payload;}

        public int serializedSize() {
            return Integer.BYTES + Bits.sizeUTF(payload) +1;
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(type.ordinal());
            Bits.writeString(payload, out);
        }

        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            this.type=Type.values()[in.readInt()];
            this.payload=Bits.readString(in);
        }

        public String toString() {
            return String.format("%s: %s", type, payload);
        }
    }
}
