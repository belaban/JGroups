package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.ChannelException;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Bela Ban
 * @version $Id: AddDataTest.java,v 1.1 2003/09/23 00:41:13 belaban Exp $
 */
public class AddDataTest extends TestCase {

    public AddDataTest(String name) {
        super(name);
        Trace.init();
    }

    public void testAdditionalData() {
        try {
            JChannel c=new JChannel();
            Map m=new HashMap();
            m.put("additional_data", new byte[]{'b', 'e', 'l', 'a'});
            c.down(new Event(Event.CONFIG, m));
            c.connect("bla");
            IpAddress addr=(IpAddress)c.getLocalAddress();
            System.out.println("address is " + addr);
            assertNotNull(addr.getAdditionalData());
            assertEquals(addr.getAdditionalData()[0], 'b');
            c.close();
        }
        catch(ChannelException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={AddDataTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
