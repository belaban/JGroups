package org.jgroups.tests;

import org.jgroups.*;

import java.util.List;
import java.util.LinkedList;

/**
 * Tests unicast functionality
 * @author Bela Ban
 * @version $Id: UnicastUnitTest.java,v 1.2 2007/07/21 06:09:21 belaban Exp $
 */
public class UnicastUnitTest extends ChannelTestBase {
    JChannel ch1, ch2=null;




    protected void setUp() throws Exception {
        super.setUp();
        ch1=createChannel();
        ch2=createChannel();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(ch2 != null)
            ch2.close();
        if(ch1 != null)
            ch1.close();
    }



    public void testUnicastMessageInCallbackExistingMember() throws Exception {
        ch1.connect("x");
        MyReceiver receiver=new MyReceiver(ch1);
        ch1.setReceiver(receiver);
        ch2.connect("x");
        Exception ex=receiver.getEx();
        if(ex != null)
            throw ex;
        ch1.setReceiver(null);
    }


    private static class MyReceiver extends ReceiverAdapter {
        Channel channel;
        Exception ex;

        public MyReceiver(Channel channel) {
            this.channel=channel;
        }

        public Exception getEx() {
            return ex;
        }

        public void viewAccepted(View new_view) {
            Address local_addr=channel.getLocalAddress();
            assertNotNull(local_addr);
            System.out.println("[" + local_addr + "]: " + new_view);
            List<Address> members=new LinkedList<Address>(new_view.getMembers());
            assertEquals("members=" + members + ", local_addr=" + local_addr, 2, members.size());
            members.remove(local_addr);
            assertEquals(1, members.size());
            Address dest=members.get(0);
            Message unicast_msg=new Message(dest, null, null);
            try {
                // uncomment line below for workaround
                // channel.down(new Event(Event.ENABLE_UNICASTS_TO, dest));
                channel.send(unicast_msg);
            }
            catch(Exception e) {
                ex=e;
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={UnicastLoopbackTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}