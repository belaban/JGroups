package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Tests a merge between partitions {A,B,C} and {D,E,F} merge, but 1 member in each partition is already involved
 * in a different merge (GMS.merge_id != null). For example, if C and E are busy with a different merge, the MergeView
 * should exclude them: {A,B,D,F}. The digests must also exclude C and E.
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MergeTest3 {
    protected MyDiagnosticsHandler handler;
    protected JChannel a,b,c,d,e,f;



    @BeforeMethod
    void setUp() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory("", false));
        handler.start();
        
        TimeScheduler timer=new TimeScheduler2(new DefaultThreadFactory("Timer", true, true),
                                               5,10,
                                               3000, 1000, "abort");

        ThreadPoolExecutor oob_thread_pool=new ThreadPoolExecutor(5, 20, 3000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000));
        oob_thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        ThreadPoolExecutor thread_pool=new ThreadPoolExecutor(5, 10, 3000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000));
        thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());

        a=createChannel("A", timer, thread_pool, oob_thread_pool);
        b=createChannel("B", timer, thread_pool, oob_thread_pool);
        c=createChannel("C", timer, thread_pool, oob_thread_pool);
        d=createChannel("D", timer, thread_pool, oob_thread_pool);
        e=createChannel("E", timer, thread_pool, oob_thread_pool);
        f=createChannel("F", timer, thread_pool, oob_thread_pool);
    }


    protected JChannel createChannel(String name, TimeScheduler timer, Executor thread_pool, Executor oob_thread_pool) throws Exception {
        SHARED_LOOPBACK shared_loopback=(SHARED_LOOPBACK)new SHARED_LOOPBACK().setValue("enable_bundling", false);
        shared_loopback.setTimer(timer);
        shared_loopback.setOOBThreadPool(oob_thread_pool);
        shared_loopback.setDefaultThreadPool(thread_pool);
        shared_loopback.setDiagnosticsHandler(handler);

        JChannel retval=Util.createChannel(shared_loopback,
                                           new DISCARD().setValue("discard_all",true),
                                           new PING().setValue("timeout",100),
                                           new NAKACK2().setValue("use_mcast_xmit",false)
                                             .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false),
                                           new UNICAST(),
                                           new STABLE().setValue("max_bytes",50000),
                                           new GMS().setValue("print_local_addr",false)
                                             .setValue("leave_timeout",100)
                                             .setValue("merge_timeout",5000)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",50)
                                             .setValue("log_collect_msgs",false));
        retval.setName(name);
        retval.connect("MergeTest3");
        JmxConfigurator.registerChannel(retval, Util.getMBeanServer(), name, retval.getClusterName(), true);
        return retval;
    }


    @AfterMethod
    void tearDown() throws Exception {
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
            ProtocolStack stack=ch.getProtocolStack();
            String cluster_name=ch.getClusterName();
            stack.stopStack(cluster_name);
            stack.destroy();
        }
        handler.destroy();
    }


    public void testMergeWithMissingMergeResponse() {
        createPartition(a,b,c);
        createPartition(d,e,f);

        System.out.println("Views are:");
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f))
            System.out.println(ch.getAddress() + ": " + ch.getView());

        JChannel merge_leader=findMergeLeader(a,b,c,d,e,f);
        List<Address> first_partition=getMembers(a,b,c);
        List<Address> second_partition=getMembers(d,e,f);

        Collections.sort(first_partition);
        Address first_coord=first_partition.remove(0); // remove the coord
        Address busy_first=first_partition.get(0);

        Collections.sort(second_partition);
        Address second_coord=second_partition.remove(0);
        Address busy_second=second_partition.get(second_partition.size() -1);

        System.out.println("\nMerge leader: " + merge_leader.getAddress() + "\nBusy members: " + Arrays.asList(busy_first, busy_second));

        MergeId busy_merge_id=MergeId.create(a.getAddress());
        setMergeIdIn(busy_first, busy_merge_id);
        setMergeIdIn(busy_second, busy_merge_id);
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) { // excluding faulty member, as it still discards messages
            assert ch.getView().size() == 3;
            Discovery ping=(Discovery)ch.getProtocolStack().findProtocol(PING.class);
            ping.setTimeout(3000);
            DISCARD discard=(DISCARD)ch.getProtocolStack().findProtocol(DISCARD.class);
            discard.setDiscardAll(false);
        }

        System.out.println("Injecting MERGE event into merge leader " + merge_leader.getAddress());
        Map<Address,View> merge_views=new HashMap<Address,View>(6);
        merge_views.put(first_coord, findChannel(first_coord).getView());
        merge_views.put(second_coord, findChannel(second_coord).getView());

        GMS gms=(GMS)merge_leader.getProtocolStack().findProtocol(GMS.class);
        // gms.setLevel("trace");
        gms.up(new Event(Event.MERGE, merge_views));

        for(int i=0; i < 20; i++) {
            boolean done=true;
            System.out.println();
            for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
                System.out.println("==> " + ch.getAddress() + ": " + ch.getView());
                Address addr=ch.getAddress();
                if(addr.equals(busy_first) || addr.equals(busy_second)) {
                    if(ch.getView().size() != 3)
                        done=false;
                }
                else {
                    if(ch.getView().size()  != 4)
                        done=false;
                }
            }
            
            if(done)
                break;
            Util.sleep(3000);
        }
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
            if(ch.getAddress().equals(busy_first) || ch.getAddress().equals(busy_second))
                assert ch.getView().size() == 3;
            else
                assert ch.getView().size() == 4 : ch.getAddress() + "'s view: " + ch.getView();
        }



        Util.sleep(1000);
        System.out.println("\n************************ Now merging the entire cluster ****************");
        cancelMerge(busy_first);
        cancelMerge(busy_second);

        System.out.println("Injecting MERGE event into merge leader " + merge_leader.getAddress());
        merge_views=new HashMap<Address,View>(6);
        merge_views.put(merge_leader.getAddress(), merge_leader.getView());
        merge_views.put(busy_first, findChannel(busy_first).getView());
        merge_views.put(busy_second, findChannel(busy_second).getView());

        System.out.println("merge event is " + merge_views);

        gms=(GMS)merge_leader.getProtocolStack().findProtocol(GMS.class);
        // gms.setLevel("trace");
        gms.up(new Event(Event.MERGE, merge_views));

        for(int i=0; i < 20; i++) {
            boolean done=true;
            System.out.println();
            for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
                System.out.println("==> " + ch.getAddress() + ": " + ch.getView());
                if(ch.getView().size()  != 6)
                    done=false;
            }

            if(done)
                break;
            Util.sleep(3000);
        }
        for(JChannel ch: new JChannel[]{a,b,c,d,e,f}) {
            if(ch.getAddress().equals(busy_first) || ch.getAddress().equals(busy_second))
                assert ch.getView().size() == 6 : ch.getAddress() + "'s view: " + ch.getView();
        }
    }

    protected void setMergeIdIn(Address mbr, MergeId busy_merge_id) {
        GMS gms=(GMS)findChannel(mbr).getProtocolStack().findProtocol(GMS.class);
        gms.getMerger().setMergeId(null, busy_merge_id);
    }

    protected void cancelMerge(Address mbr) {
        GMS gms=(GMS)findChannel(mbr).getProtocolStack().findProtocol(GMS.class);
        gms.cancelMerge();
    }

    protected JChannel findChannel(Address mbr) {
        for(JChannel ch: Arrays.asList(a,b,c,d,e,f)) {
            if(ch.getAddress().equals(mbr))
                return ch;
        }
        return null;
    }

    protected void createPartition(JChannel ... channels) {
        List<Address> members=getMembers(channels);
        Collections.sort(members);
        Address coord=members.get(0);
        View view=new View(coord, 2, members);
        MutableDigest digest=new MutableDigest(3);
        for(JChannel ch: channels) {
            NAKACK2 nakack=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
            digest.merge(nakack.getDigest(ch.getAddress()));
        }
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view, digest);
        }
    }

    protected List<Address> getMembers(JChannel ... channels) {
        List<Address> members=new ArrayList<Address>(channels.length);
        for(JChannel ch: channels)
            members.add(ch.getAddress());
        return members;
    }

    protected Address determineCoordinator(JChannel ... channels) {
        List<Address> list=new ArrayList<Address>(channels.length);
        for(JChannel ch: channels)
            list.add(ch.getAddress());
        Collections.sort(list);
        return list.get(0);
    }

    protected JChannel findMergeLeader(JChannel ... channels) {
        Set<Address> tmp=new TreeSet<Address>();
        for(JChannel ch: channels)
            tmp.add(ch.getAddress());
        Address leader=tmp.iterator().next();
        for(JChannel ch: channels)
            if(ch.getAddress().equals(leader))
                return ch;
        return null;
    }


    protected static class MyDiagnosticsHandler extends DiagnosticsHandler {

        protected MyDiagnosticsHandler(InetAddress diagnostics_addr, int diagnostics_port, Log log, SocketFactory socket_factory, ThreadFactory thread_factory) {
            super(diagnostics_addr,diagnostics_port,log,socket_factory,thread_factory);
        }

        public void start() throws IOException {super.start();}
        public void stop() {}
        public void destroy() {super.stop();}
    }

    @Test(enabled=false)
    public static void main(String[] args) throws Exception {
        MergeTest3 test=new MergeTest3();
        test.setUp();
        test.testMergeWithMissingMergeResponse();
        test.tearDown();
    }


  
}
