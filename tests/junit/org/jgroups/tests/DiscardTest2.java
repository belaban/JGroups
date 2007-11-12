package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.MERGE2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: DiscardTest2.java,v 1.1 2007/11/12 13:53:08 belaban Exp $
 */
public class DiscardTest2 extends ChannelTestBase {
    JChannel ch1, ch2, ch3;

    protected void tearDown() throws Exception {
        closeChannel(ch3, ch2, ch1);
        super.tearDown();
    }

    /** Simluates pulling the plug and re-inserting it later */
    public void testHangOfParticipant() throws Exception {
        ch1=createChannel();
        ch2=createChannel();

        addDiscardProtocol(ch1);
        modiftFDAndMergeSettings(ch1);
        modiftFDAndMergeSettings(ch2);

        ch1.connect("x");
        ch2.connect("x");

        View view=ch2.getView();
        assertEquals(2, view.size());
        System.out.println("view is " + view + "\nSuspending ch1");
        DISCARD discard=(DISCARD)ch1.getProtocolStack().findProtocol("DISCARD");
        discard.setDiscardAll(true);

        Util.sleep(8000);
        view=ch2.getView();
        System.out.println("view is " + view);
        assertEquals(1, view.size());
        view=ch1.getView();
        assertEquals(1, view.size());

        System.out.println("Resuming ch1");
        discard.setDiscardAll(false);

        Util.sleep(3000);
        for(int i=0; i < 10; i++) {
            view=ch2.getView();
            System.out.println("view after resuming: " + view);
            if(view.size() == 2) {
                break;
            }
            Util.sleep(1000);
        }
        assertEquals(2, view.size());
    }




    private static void closeChannel(Channel... channel) {
        for(Channel ch: channel) {
            if(ch != null && !ch.isConnected())
                ch.close();
        }
    }


    private static void addDiscardProtocol(JChannel ch) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol transport=stack.getTransport();
        DISCARD discard=new DISCARD();
        discard.setProtocolStack(ch.getProtocolStack());
        discard.start();
        stack.insertProtocol(discard, ProtocolStack.ABOVE, transport.getName());
    }

    private static void modiftFDAndMergeSettings(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();

        FD fd=(FD)stack.findProtocol("FD");
        if(fd != null) {
            fd.setMaxTries(3);
            fd.setTimeout(1000);
        }
        MERGE2 merge=(MERGE2)stack.findProtocol("MERGE2");
        if(merge != null) {
            merge.setMinInterval(5000);
            merge.setMaxInterval(10000);
        }

        ch.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View new_view) {
                System.out.println("** view: " + new_view);
            }
        });
    }

}
