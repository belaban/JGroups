package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.jgroups.util.Util.printViews;


/**
 * Tests https://issues.jboss.org/browse/JGRP-2092:
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MergeTest5 {
    protected JChannel a,b,c;

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel("A");
        b=createChannel("B");
        c=createChannel("C");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);
    }

    @AfterMethod void tearDown() throws Exception {Util.close(a,b,c);}

    /**
     * Tests https://issues.jboss.org/browse/JGRP-2092:
     <pre>
     Host A view: B,A,C (where B should be coordinator)
     Host B view: C,A,B (where C should be coordinator)
     Host C view: A,B,C (where A should be coordinator)
     </pre>
     */
    public void testSplitWithNoCoordinator() throws Exception {
        System.out.printf("Initial views:\n%s\n", printViews(a,b,c));

        View v1=createView(5, b,a,c);
        View v2=createView(5, c,a,b);

        System.out.printf("Injecting view %s into %s and %s into %s\n",
                          v1, a.getName(), v2, b.getName());
        injectView(v1, a);
        injectView(v2, b);

        System.out.printf("\nViews after injection:\n%s\n", printViews(a,b,c));

        for(int x=0; x < 20; x++) {
            View first=a.getView();
            if(first.equals(b.getView()) && first.equals(c.getView()))
                break;

            for(JChannel ch: Arrays.asList(a,b,c)) {
                MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
                merge.sendInfo(); // multicasts an INFO msg to everybody else
            }
            Util.sleep(1000);
        }
        System.out.printf("\nFinal views:\n%s\n", printViews(a,b,c));
        Util.assertAllChannelsHaveSameView(a,b,c);
    }




    protected static View createView(int id, JChannel... mbrs) {
        Address[] members=new Address[mbrs.length];
        for(int i=0; i < mbrs.length; i++)
            members[i]=mbrs[i].getAddress();
        return View.create(mbrs[0].getAddress(), id, members);
    }



    protected static JChannel createChannel(String name) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            new MERGE3().setMinInterval(3000).setMaxInterval(4000).setCheckInterval(7000),
                            new NAKACK2().useMcastXmit(false)
                                       .logDiscardMessages(false).logNotFoundMessages(false),
                            new UNICAST3(),
                            new STABLE().setMaxBytes(50000),
                            new GMS().printLocalAddress(false)
                                       .setJoinTimeout( 100)
                                       .setLeaveTimeout(100)
                                       .setMergeTimeout(5000)
                                       .logViewWarnings(false)
                                       .setViewAckCollectionTimeout(50)
                                       .logCollectMessages(false))
          .name(name).connect("MergeTest5");
    }



    protected static void injectView(View view, JChannel... channels) {
        for(JChannel ch: channels) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

    protected static void checkInconsistencies(JChannel... channels) {
        for(JChannel ch: channels) {
            MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
            merge.checkInconsistencies();
        }
    }


}
