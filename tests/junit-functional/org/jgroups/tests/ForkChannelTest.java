package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.ReplicatedHashMap;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.fork.ForkProtocolStack;
import org.jgroups.protocols.COUNTER;
import org.jgroups.protocols.FORK;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.pbcast.STATE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests {@link org.jgroups.fork.ForkChannel}
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ForkChannelTest {
    protected JChannel                a, b;
    protected ForkChannel             fc1, fc2, fc3, fc4;
    protected static final String     CLUSTER="ForkChannelTest";

    protected Protocol[] protocols() {return Util.getTestStack(new STATE(), new FORK());}

    @BeforeMethod protected void setup() throws Exception {
        a=new JChannel(protocols()).name("A");
    }

    @AfterMethod protected void destroy() {
        Util.close(fc4, fc3, fc2, fc1, a, b);
    }


    @Test
    public void testCreateForkIfAbsent() throws Exception {
        JChannel c = new JChannel(Util.getTestStack(new STATE())).name("C");

        ForkChannel fc = new ForkChannel(c,
                "hijack-stack",
                "lead-hijacker",
                true,
                ProtocolStack.ABOVE,
                FRAG2.class);
        assert fc.isOpen() && !fc.isConnected() && !fc.isClosed() : "state=" + fc.getState();

        Util.close(fc, c);
    }

    public void testLifecycle() throws Exception {
        fc1=new ForkChannel(a, "stack", "fc1");
        assert fc1.isOpen() && !fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        a.connect(CLUSTER);
        assert fc1.isOpen() && !fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        fc1.connect("bla");
        assert fc1.isOpen() && fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        assert a.getAddress().equals(fc1.getAddress());
        assert a.getClusterName().equals(fc1.getClusterName());
        assert a.getView().equals(fc1.getView());

        fc1.disconnect();
        assert fc1.isOpen() && !fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        fc1.connect("foobar");
        assert fc1.isOpen() && fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        Util.close(fc1);
        assert !fc1.isOpen() && !fc1.isConnected() && fc1.isClosed() : "state=" + fc1.getState();

        try {
            fc1.connect("whocares");
            assert false : "a closed fork channel cannot be reconnected";
        }
        catch(Exception ex) {
            assert ex instanceof IllegalStateException;
        }
        assert !fc1.isOpen() && !fc1.isConnected() && fc1.isClosed() : "state=" + fc1.getState();

        Util.close(a);
        assert !fc1.isOpen() && !fc1.isConnected() && fc1.isClosed() : "state=" + fc1.getState();

        try {
            fc1.send(null, "hello");
            assert false: "sending on a fork-channel with a disconnected main-channel should throw an exception";
        }
        catch(Throwable t) {
            System.out.println("got an exception (as expected) sending on a fork-channel where the main-channel is disconnected: " + t);
        }
    }


    public void testIncorrectConnectSequence() throws Exception {
        fc1=new ForkChannel(a, "stack", "fc1");
        try {
            fc1.connect(CLUSTER);
            assert false : "Connecting a fork channel before the main channel should have thrown an exception";
        }
        catch(Exception ex) {
            assert ex instanceof IllegalStateException : "expected IllegalStateException but got " + ex;
        }
    }

    public void testRefcount() throws Exception {
        FORK fork=(FORK)a.getProtocolStack().findProtocol(FORK.class);
        Protocol prot=fork.get("stack");
        assert prot == null;
        fc1=new ForkChannel(a, "stack", "fc1");
        prot=fork.get("stack");
        assert prot != null;
        ForkProtocolStack fork_stack=(ForkProtocolStack)getProtStack(prot);
        int inits=fork_stack.getInits();
        assert inits == 1 : "inits is " + inits + "(expected 1)";

        fc2=new ForkChannel(a, "stack", "fc2"); // uses the same fork stack "stack"
        inits=fork_stack.getInits();
        assert inits == 2 : "inits is " + inits + "(expected 2)";

        a.connect(CLUSTER);

        fc1.connect(CLUSTER);
        int connects=fork_stack.getConnects();
        assert connects == 1 : "connects is " + connects + "(expected 1)";

        fc1.connect(CLUSTER); // duplicate connect()
        connects=fork_stack.getConnects();
        assert connects == 1 : "connects is " + connects + "(expected 1)";

        fc2.connect(CLUSTER);
        connects=fork_stack.getConnects();
        assert connects == 2 : "connects is " + connects + "(expected 2)";

        fc2.disconnect();
        fc2.disconnect(); // duplicate disconnect() !
        connects=fork_stack.getConnects();
        assert connects == 1 : "connects is " + connects + "(expected 1)";

        Util.close(fc2);
        inits=fork_stack.getInits();
        assert inits == 1 : "inits is " + inits + "(expected 1)";

        Util.close(fc2); // duplicate close()
        inits=fork_stack.getInits();
        assert inits == 1 : "inits is " + inits + "(expected 1)";

        Util.close(fc1);
        connects=fork_stack.getConnects();
        assert connects == 0 : "connects is " + connects + "(expected 0)";
        inits=fork_stack.getInits();
        assert inits == 0 : "inits is " + inits + "(expected 0)";

        prot=fork.get("stack");
        assert prot == null;
    }

    public void testRefcount2() throws Exception {
        Prot p1=new Prot("P1"), p2=new Prot("P2");
        fc1=new ForkChannel(a, "stack", "fc1", p1, p2);
        fc2=new ForkChannel(a, "stack", "fc2"); // uses p1 and p2 from fc1
        fc3=new ForkChannel(a, "stack", "fc3"); // uses p1 and p2 from fc1

        assert p1.inits == 1 && p2.inits == 1;

        FORK fork=(FORK)a.getProtocolStack().findProtocol(FORK.class);
        Protocol prot=fork.get("stack");
        ForkProtocolStack fork_stack=(ForkProtocolStack)getProtStack(prot);
        int inits=fork_stack.getInits();
        assert inits == 3;

        a.connect(CLUSTER);
        fc1.connect(CLUSTER);
        int connects=fork_stack.getConnects();
        assert connects == 1;
        assert p1.starts == 1 && p2.starts == 1;

        fc2.connect(CLUSTER);
        fc3.connect(CLUSTER);
        connects=fork_stack.getConnects();
        assert connects == 3;
        assert p1.starts == 1 && p2.starts == 1;

        fc3.disconnect();
        fc2.disconnect();
        assert p1.starts == 1 && p2.starts == 1;
        assert p1.stops == 0 && p2.stops == 0;

        fc1.disconnect();
        assert p1.starts == 1 && p2.starts == 1;
        assert p1.stops == 1 && p2.stops == 1;

        Util.close(fc3,fc2);
        assert p1.destroys == 0 && p2.destroys == 0;

        Util.close(fc1);
        assert p1.destroys == 1 && p2.destroys == 1;
    }


    public void testIncorrectLifecycle() throws Exception {
        fc1=new ForkChannel(a, "stack", "fc1");
        a.connect(CLUSTER);
        fc1.connect(CLUSTER);
        Util.close(fc1);
        try {
            fc1.connect(CLUSTER);
            assert false : "reconnecting a closed fork channel must throw an exception";
        }
        catch(Exception ex) {
            assert ex instanceof IllegalStateException;
            System.out.println("got exception as expected: " + ex);
        }
    }


    /** Tests the case where we don't add any fork-stack specific protocols */
    public void testNullForkStack() throws Exception {
        fc1=new ForkChannel(a, "stack", "fc1");
        fc2=new ForkChannel(a, "stack", "fc2");
        MyReceiver<Integer> r1=new MyReceiver<>(), r2=new MyReceiver<>();
        fc1.setReceiver(r1); fc2.setReceiver(r2);
        a.connect(CLUSTER);
        fc1.connect("foo");
        fc2.connect("bar");

        for(int i=1; i <= 5; i++) {
            fc1.send(null, i);
            fc2.send(null, i+5);
        }

        List<Integer> l1=r1.list(), l2=r2.list();
        for(int i=0; i < 20; i++) {
            if(l1.size() == 5 && l2.size() == 5)
                break;
            Util.sleep(500);
        }

        System.out.println("r1: " + r1.list() + ", r2: " + r2.list());
        assert r1.size() == 5 && r2.size() == 5;
        for(int i=1; i <= 5; i++)
            assert r1.list().contains(i) && r2.list().contains(i+5);
    }


    /**
     * Tests CounterService on 2 different fork-channels, using the *same* fork-stack. This means the 2 counter
     * services will 'see' each other and the counters must have the same value
     * @throws Exception
     */
    public void testCounterService() throws Exception {
        a.connect(CLUSTER);
        fc1=new ForkChannel(a, "stack", "fc1", false,ProtocolStack.ABOVE, FORK.class, new COUNTER());
        fc2=new ForkChannel(a, "stack", "fc2", false,ProtocolStack.ABOVE, FORK.class, new COUNTER());

        fc1.connect("foo");
        fc2.connect("bar");

        CounterService cs1=new CounterService(fc1), cs2=new CounterService(fc2);

        Counter c1=cs1.getOrCreateCounter("counter", 1), c2=cs2.getOrCreateCounter("counter", 1);
        System.out.println("counter1=" + c1 + ", counter2=" + c2);
        assert c1.get() == 1 && c2.get() == 1;

        c1.addAndGet(5);
        System.out.println("counter1=" + c1 + ", counter2=" + c2);
        assert c1.get() == 6 && c2.get() == 6;

        c2.compareAndSet(6, 10);
        System.out.println("counter1=" + c1 + ", counter2=" + c2);
        assert c1.get() == 10 && c2.get() == 10;
    }


    public void testStateTransfer() throws Exception {
        ReplicatedHashMap<String,Integer> rhm_a=new ReplicatedHashMap<>(a),
          rhm_b, rhm_fc1, rhm_fc2, rhm_fc3, rhm_fc4;

        a.connect("state-transfer");
        fc1=createForkChannel(a, "stack1", "fc1");
        rhm_fc1=new ReplicatedHashMap<>(fc1);
        fc2=createForkChannel(a, "stack2", "fc2");
        rhm_fc2=new ReplicatedHashMap<>(fc2);
        addData(rhm_a, rhm_fc1, rhm_fc2);

        b=new JChannel(protocols()).name("B");
        rhm_b=new ReplicatedHashMap<>(b);
        b.connect("state-transfer");
        fc3=createForkChannel(b, "stack1", "fc1");
        rhm_fc3=new ReplicatedHashMap<>(fc3);
        fc4=createForkChannel(b, "stack2", "fc2");
        rhm_fc4=new ReplicatedHashMap<>(fc4);

        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
        b.getState(null, 10000);

        for(int i=0; i < 10; i++) {
            if(rhm_b.size() == rhm_a.size() && rhm_fc1.size() == rhm_fc3.size() && rhm_fc2.size() == rhm_fc4.size())
                break;
            Util.sleep(1000);
        }
        System.out.printf("rhm_a: %s, rhm_b: %s\nrhm_fc1: %s, rhm_fc3: %s\nrhm_fc2: %s, rhm_fc4: %s\n",
                          rhm_a, rhm_b, rhm_fc1, rhm_fc3, rhm_fc2, rhm_fc4);
        assert rhm_a.equals(rhm_b);
        assert rhm_fc1.equals(rhm_fc3);
        assert rhm_fc2.equals(rhm_fc4);
    }


    protected ForkChannel createForkChannel(JChannel main, String stack_name, String ch_name) throws Exception {
        ForkChannel fork_ch=new ForkChannel(main, stack_name, ch_name);
        fork_ch.connect(ch_name);
        return fork_ch;
    }

    protected void addData(ReplicatedHashMap<String,Integer> a, ReplicatedHashMap<String,Integer> b, ReplicatedHashMap<String,Integer> c) {
        if(a != null) {
            a.put("id", 322649);
            a.put("version", 45);
        }
        if(b != null) {
            b.put("major", 3);
            b.put("minor", 6);
            b.put("patch", 5);
        }
        if(c != null) {
            c.put("hobbies", 3);
            c.put("kids", 2);
        }
    }


    protected static ProtocolStack getProtStack(Protocol prot) {
        while(prot != null && !(prot instanceof ProtocolStack)) {
            prot=prot.getUpProtocol();
        }
        return prot instanceof ProtocolStack? (ProtocolStack)prot : null;
    }

  /*  protected Entry[] create(boolean main_ch, boolean fc1, boolean fc2) {
        Entry[] entry=new Entry[3];

        return entry;
    }


    protected static class Entry {
        protected final JChannel                          ch;
        protected final ReplicatedHashMap<String,Integer> map;

        public Entry(JChannel ch, ReplicatedHashMap<String,Integer> map) {
            this.ch=ch;
            this.map=map;
        }
    }*/

    protected static class Prot extends Protocol {
        protected final String myname;
        protected int          inits, starts, stops, destroys;

        public Prot(String name) {
            this.myname=name;
        }

        public void init() throws Exception {
            super.init();
            System.out.println(myname + ".init()");
            inits++;
        }

        public void start() throws Exception {
            super.start();
            System.out.println(myname + ".start()");
            starts++;
        }

        public void stop() {
            super.stop();
            System.out.println(myname + ".stop()");
            stops++;
        }

        public void destroy() {
            super.destroy();
            System.out.println(myname + ".destroy()");
            destroys++;
        }

        public Object down(Event evt) {
            System.out.println(myname + ": down(): " + evt);
            return down_prot.down(evt);
        }

        public String toString() {
            return myname;
        }
    }
}
