package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
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
 * Tests a merge where {A}, {B}, {C} and {D} are found, and then one of the members doesn't answer the merge request.
 * Goal: the merge should *not* get cancelled, but instead all 3 non-faulty member should merge
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MergeTest2 {
    protected MyDiagnosticsHandler handler;
    protected JChannel a,b,c,d;



    @BeforeMethod
    void setUp() throws Exception {
        handler=new MyDiagnosticsHandler(InetAddress.getByName("224.0.75.75"), 7500,
                                         LogFactory.getLog(DiagnosticsHandler.class),
                                         new DefaultSocketFactory(),
                                         new DefaultThreadFactory(Util.getGlobalThreadGroup(), "", false));
        handler.start();
        
        ThreadGroup test_group=new ThreadGroup("MergeTest2");
        TimeScheduler timer=new TimeScheduler2(new DefaultThreadFactory(test_group, "Timer", true, true),
                                               5,10,
                                               3000, 1000, "abort");

        ThreadPoolExecutor oob_thread_pool=new ThreadPoolExecutor(5, 50, 3000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000));
        oob_thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        ThreadPoolExecutor thread_pool=new ThreadPoolExecutor(5, 10, 3000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(10000));
        thread_pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());

        a=createChannel("A", timer, thread_pool, oob_thread_pool);
        b=createChannel("B", timer, thread_pool, oob_thread_pool);
        c=createChannel("C", timer, thread_pool, oob_thread_pool);
        d=createChannel("D", timer, thread_pool, oob_thread_pool);
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
                                           new NAKACK().setValue("use_mcast_xmit",false)
                                             .setValue("log_discard_msgs",false).setValue("log_not_found_msgs",false),
                                           new UNICAST(),
                                           new STABLE().setValue("max_bytes",50000),
                                           new GMS().setValue("print_local_addr",false)
                                             .setValue("leave_timeout",100)
                                             .setValue("merge_timeout",3000)
                                             .setValue("log_view_warnings",false)
                                             .setValue("view_ack_collection_timeout",50)
                                             .setValue("log_collect_msgs",false));
        retval.setName(name);
        JmxConfigurator.registerChannel(retval, Util.getMBeanServer(), name, retval.getClusterName(), true);
        retval.connect("MergeTest2");
        return retval;
    }


    @AfterMethod
    void tearDown() throws Exception {
        for(JChannel ch: new JChannel[]{a,b,c,d}) {
            ProtocolStack stack=ch.getProtocolStack();
            String cluster_name=ch.getClusterName();
            stack.stopStack(cluster_name);
            stack.destroy();
        }
        handler.destroy();
    }


    public void testMergeWithMissingMergeResponse() {
        JChannel merge_leader=findMergeLeader(a,b,c,d);
        List<Address> non_faulty_members=new ArrayList<Address>();
        non_faulty_members.addAll(Arrays.asList(a.getAddress(),b.getAddress(),c.getAddress(),d.getAddress()));
        List<Address> tmp=new ArrayList<Address>(non_faulty_members);
        tmp.remove(merge_leader.getAddress());
        Address faulty_member=(Address)Util.pickRandomElement(tmp);
        non_faulty_members.remove(faulty_member);

        System.out.println("\nMerge leader: " + merge_leader.getAddress() + "\nFaulty member: " + faulty_member +
                             "\nNon-faulty members: " + non_faulty_members);

        for(JChannel ch: new JChannel[]{a,b,c,d}) { // excluding faulty member, as it still discards messages
            assert ch.getView().size() == 1;
            if(ch.getAddress().equals(faulty_member)) // skip the faulty member; it keeps discarding messages
                continue;
            DISCARD discard=(DISCARD)ch.getProtocolStack().findProtocol(DISCARD.class);
            discard.setDiscardAll(false);
        }

        Map<Address,View> merge_views=new HashMap<Address,View>(4);
        for(JChannel ch: new JChannel[]{a,b,c,d}) {
            merge_views.put(ch.getAddress(), ch.getView()); // here, we include D
        }

        System.out.println("Injecting MERGE event into merge leader " + merge_leader.getAddress());
        GMS gms=(GMS)merge_leader.getProtocolStack().findProtocol(GMS.class);
        gms.setLevel("trace");
        gms.up(new Event(Event.MERGE, merge_views));


        for(int i=0; i < 20; i++) {
            boolean done=true;
            System.out.println();
            for(JChannel ch: new JChannel[]{a,b,c,d}) {
                if(!ch.getAddress().equals(faulty_member)) {
                    System.out.println("==> " + ch.getAddress() + ": " + ch.getView());
                    if(ch.getView().size() != 3)
                        done=false;
                }
            }
            
            if(done)
                break;
            Util.sleep(3000);
        }

        for(JChannel ch: new JChannel[]{a,b,c,d}) {
            if(ch.getAddress().equals(faulty_member))
                assert ch.getView().size() == 1;
            else
                assert ch.getView().size() == 3 : ch.getAddress() + "'s view: " + ch.getView();
        }
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

        public void start() throws IOException {
            super.start();
        }

        public void stop() {
        }

        public void destroy() {
            super.stop();
        }
    }


  
}
