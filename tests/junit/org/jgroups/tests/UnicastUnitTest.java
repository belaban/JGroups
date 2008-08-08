package org.jgroups.tests;

import org.jgroups.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests unicast functionality
 * @author Bela Ban
 * @version $Id: UnicastUnitTest.java,v 1.7 2008/08/08 17:07:12 vlada Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=false)
public class UnicastUnitTest extends ChannelTestBase {
    JChannel ch1, ch2=null;


    @BeforeMethod
    protected void setUp() throws Exception {
        ch1=createChannel(true,2);
        ch2=createChannel(ch1);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        if(ch2 != null)
            ch2.close();
        if(ch1 != null)
            ch1.close();
    }



    @Test
    public void testUnicastMessageInCallbackExistingMember() throws Exception {
        ch1.connect("UnicastUnitTest");
        MyReceiver receiver=new MyReceiver(ch1);
        ch1.setReceiver(receiver);
        ch2.connect("UnicastUnitTest");
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
}