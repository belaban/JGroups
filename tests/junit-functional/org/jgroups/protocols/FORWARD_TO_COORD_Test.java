package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests the FORWARD_TO_COORD protocol
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},singleThreaded=true)
public class FORWARD_TO_COORD_Test {
    protected static final int     NUM=3; // number of members
    protected static final int     BASE='A';
    protected final JChannel[]     channels=new JChannel[NUM];
    protected final MyReceiver[]   receivers=new MyReceiver[NUM];



    @BeforeMethod
    void setUp() throws Exception {
        System.out.print("Connecting channels: ");
        for(int i=0; i < NUM; i++) {
            channels[i]=new JChannel(new SHARED_LOOPBACK(),
                                     new DISCARD(),
                                     new SHARED_LOOPBACK_PING(),
                                     new NAKACK2().setValue("use_mcast_xmit",false)
                                       .setValue("discard_delivered_msgs",true)
                                       .setValue("log_discard_msgs",true).setValue("log_not_found_msgs",true)
                                       .setValue("xmit_table_num_rows",5)
                                       .setValue("xmit_table_msgs_per_row",10),
                                     new UNICAST3().setValue("xmit_table_num_rows",5).setValue("xmit_interval", 300)
                                       .setValue("xmit_table_msgs_per_row",10)
                                       .setValue("conn_expiry_timeout", 10000),
                                     new GMS().setValue("print_local_addr",false)
                                       .setValue("leave_timeout",2000)
                                       .setValue("log_view_warnings",false)
                                       .setValue("view_ack_collection_timeout",2000)
                                       .setValue("log_collect_msgs",false),
                                     new FORWARD_TO_COORD());
            String name=String.valueOf((char)(i + BASE));
            channels[i].setName(name);
            receivers[i]=new MyReceiver();
            channels[i].setReceiver(receivers[i]);
            channels[i].connect("FORWARD_TO_COORD_Test");
            System.out.print(name + " ");
            if(i == 0)
                Util.sleep(1500);
        }
        System.out.println("\n");
        System.out.flush();
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, channels);
    }

    @AfterMethod
    void tearDown() throws Exception {
        for(int i=NUM-1; i >= 0; i--) {
            ProtocolStack stack=channels[i].getProtocolStack();
            String cluster_name=channels[i].getClusterName();
            stack.stopStack(cluster_name);
            stack.destroy();
        }
    }


    /**
     * Tests the default case: we have {A,B,C}, with A being the coordinator. C forwards a message to the current
     * coordinator and therefore A must receive it.
     */
    public void testSimpleForwarding() throws Exception {
        Message msg=new BytesMessage(null, 22);
        channels[NUM-1].down(new Event(Event.FORWARD_TO_COORD, msg)); // send on C, A must receive it
        MyReceiver receiver=receivers[0];
        for(int i=0; i < 20; i++) {
            if(receiver.size() == 1)
                break;
            Util.sleep(500);
        }
        List<Integer> values=receiver.getValues();
        System.out.println("A: received values: " + values);
        assert values.size() == 1;
        assert values.get(0) == 22;

        for(int i=1; i < NUM; i++)
            assert receivers[i].size() == 0;
    }

    /**
     * Tests the case where C forwards a Message to A, but A leaves, so eventually B should receive C's message
     */
    public void testForwardingWithCoordLeaving() throws Exception {
        Message msg=new BytesMessage(null, 25);

        DISCARD discard=channels[NUM-1].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDropDownUnicasts(1);

        // Sends the message to A, but C will discard it, so A will never get it
        channels[NUM-1].down(new Event(Event.FORWARD_TO_COORD,msg));

        // Now A leaves, C should resend the message to B
        System.out.println("\n***** disconnecting A ******");
        Util.close(channels[0]);

        MyReceiver receiver=receivers[1]; // B
        for(int i=0; i < 20; i++) {
            if(receiver.size() == 1)
                break;
            Util.sleep(500);
        }
        System.out.println("Receivers");
        printReceivers();

        List<Integer> values=receiver.getValues();
        System.out.println("B: received values: " + values);
        assert values.size() == 1;
        assert values.get(0) == 25;
    }


    /**
     * Tests the case where C forwards a Message to A, but A leaves, so eventually B should receive C's message
     */
    public void testForwardingWithCoordCrashing() throws Exception {
        Message msg=new BytesMessage(null, 30);

        DISCARD discard=channels[0].getProtocolStack().findProtocol(DISCARD.class);
        discard.setDiscardAll(true);

        // Sends the message to A, but C will discard it, so A will never get it
        channels[NUM-1].down(new Event(Event.FORWARD_TO_COORD,msg));

        // Now A leaves, C should resend the message to B
        System.out.println("***** crashing A ******");
        Util.shutdown(channels[0]);

        View view=View.create(channels[1].getAddress(), 5, channels[1].getAddress(), channels[2].getAddress());

        System.out.println("Injecting view " + view + " into B and C");
        for(JChannel ch: Arrays.asList(channels[1], channels[2])) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.up(new Event(Event.VIEW_CHANGE, view));
        }

        MyReceiver receiver=receivers[1]; // B
        for(int i=0; i < 20; i++) {
            if(receiver.size() == 1)
                break;
            Util.sleep(500);
        }
        System.out.println("Receivers");
        printReceivers();

        List<Integer> values=receiver.getValues();
        System.out.println("B: received values: " + values);
        assert values.size() == 1 : "values are " + values;
        assert values.get(0) == 30;
    }

    /**
     * Tests the case where a view is not installed at the same time in all members. C thinks B is the new coord and
     * forwards a message to B. B, however doesn't yet have the same view, so it rejects (NOT_COORD message to C) the
     * message. C in turn resends the message to B and so on. Only when B finally installs the view, will the message
     * get accepted.
     */
    public void testNotCoord() {
        View new_view=View.create(channels[1].getAddress(), 3, channels[1].getAddress(),
                                      channels[0].getAddress(), channels[2].getAddress());
        System.out.println("Installing view " + new_view + " members A and C (not B !)");
        for(JChannel ch: new JChannel[]{channels[0], channels[2]}) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.up(new Event(Event.VIEW_CHANGE, new_view));
        }

        for(JChannel ch: channels)
            System.out.println(ch.getName() + ": view is " + ch.getView());


        Message msg=new BytesMessage(null, 35);

        // Sends the message to A, but C will discard it, so A will never get it
        System.out.println("C: forwarding the message to B");
        channels[NUM-1].down(new Event(Event.FORWARD_TO_COORD,msg));

        Util.sleep(500);

        System.out.println("Injecting view " + new_view + " into B and C");
        GMS gms=channels[1].getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.VIEW_CHANGE, new_view));

        gms=channels[NUM-1].getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.VIEW_CHANGE, new_view));

        MyReceiver receiver=receivers[1]; // B
        for(int i=0; i < 20; i++) {
            if(receiver.size() == 1)
                break;
            Util.sleep(500);
        }
        System.out.println("Receivers");
        printReceivers();

        List<Integer> values=receiver.getValues();
        System.out.println("B: received values: " + values);
        assert values.size() == 1;
        assert values.get(0) == 35;
    }



    void printReceivers() {
        for(int i=0; i < NUM; i++) {
            System.out.println(channels[i].getName() + ": " + receivers[i].getValues() + ", view: " + channels[i].getView());
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected final List<Integer> values=new ArrayList<>();
        public int size()                {return values.size();}
        public List<Integer> getValues() {return values;}

        public void receive(Message msg) {values.add((Integer)msg.getObject());}

    }


}
