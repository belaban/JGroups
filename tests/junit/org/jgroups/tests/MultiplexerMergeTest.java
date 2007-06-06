package org.jgroups.tests;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.TestCase;
import org.jgroups.Channel;
import org.jgroups.ExtendedMessageListener;
import org.jgroups.JChannelFactory;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Util;

/**
 * Tests merging with a multiplexer channel
 * @author Jerry Gauthier
 * @version $Id: MultiplexerMergeTest.java,v 1.4 2007/06/06 11:05:53 belaban Exp $
 */
public class MultiplexerMergeTest extends TestCase {
	// stack file must be on classpath
    private static final String STACK_FILE = "stacks.xml";
    private static final String STACK_NAME = "tunnel";
    // router address and port must match definition in stack
    private static final int ROUTER_PORT = 12001;
    private static final String BIND_ADDR = "127.0.0.1";
    
    private JChannelFactory factory;
    private JChannelFactory factory2;
    private Channel ch1;
    private Channel ch2;
    private GossipRouter router;
    private RpcDispatcher dispatcher1;
    private RpcDispatcher dispatcher2;

    public MultiplexerMergeTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        
        factory = new JChannelFactory();
        factory.setMultiplexerConfig(STACK_FILE);

        factory2 = new JChannelFactory();
        factory2.setMultiplexerConfig(STACK_FILE);
        
        startRouter();
        
        ch1 = factory.createMultiplexerChannel(STACK_NAME, "foo");
        dispatcher1 = new RpcDispatcher(ch1, null, null, new Object(), false);
        dispatcher1.setMessageListener(new MessageListenerAdaptor("listener1", "client1 initial state"));
        ch1.connect("bla");
        ch1.getState(null, 10000);

        ch2 = factory2.createMultiplexerChannel(STACK_NAME, "foo");
        dispatcher2 = new RpcDispatcher(ch2, null, null, new Object(), false);
        dispatcher2.setMessageListener(new MessageListenerAdaptor("listener2", "client2 initial state"));
        ch2.connect("bla");
        boolean rc = ch2.getState(null, 10000);
        //assertTrue("channel2 failed to obtain state successfully", rc);
        
        System.out.println("sleeping for 5 seconds");
        Util.sleep(5000);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        ch2.close();
        ch1.close();
        stopRouter();
    }

    public void testPartitionAndSubsequentMerge() throws Exception {
        partitionAndMerge();
    }

    private void partitionAndMerge() throws Exception {
        View v = ch2.getView();
        System.out.println("ch2 view is " + v);
        assertEquals("channel2 should have 2 members", 2, ch2.getView().size());

        System.out.println("++ simulating network partition by stopping the GossipRouter");
        stopRouter();

        System.out.println("sleeping for 20 seconds");
        Util.sleep(20000);

        v = ch1.getView();
        System.out.println("-- ch1.view: " + v);
        v = ch2.getView();
        System.out.println("-- ch2.view: " + v);
        
        assertEquals("channel2 should have 1 member (channels should have excluded each other)", 1, v.size());

        System.out.println("++ simulating merge by starting the GossipRouter again");
        router.start();

        System.out.println("sleeping for 30 seconds");
        Util.sleep(30000);  

        v = ch1.getView();
        System.out.println("-- ch1.view: " + v);
        v = ch2.getView();
        System.out.println("-- ch2.view: " + v);

        assertEquals("channel2 is supposed to have 2 members again after merge", 2, ch2.getView().size());
    }

    private void startRouter() throws Exception {
        router = new GossipRouter(ROUTER_PORT, BIND_ADDR);
        router.start();
    }

    private void stopRouter() {
        router.stop();
    }

    private static final class MessageListenerAdaptor implements ExtendedMessageListener {
    	private String m_name;
        private byte[] m_state = null;
        
        MessageListenerAdaptor(String name, String state) {
        	m_name = name;
        	if (state != null)
        		m_state = state.getBytes();
        }        
        
        public void receive(Message msg) {
            System.out.println(m_name + " MultiplexerMergeTest.receive() - not implemented");    
        }
        
        public byte[] getState() {
            System.out.println(m_name + " MultiplexerMergeTest.getState() - returning byte[] state = " + new String(m_state));
            return m_state;        
        }
        
        public void setState(byte[] state) {
            System.out.println(m_name + " MultiplexerMergeTest.setState(byte[]) - setting state = " + new String(state));
            m_state = state;
        }
        
        public void setState(InputStream is) {
            m_state = getInputStreamBytes(is);
            try
            {
            	is.close();
            }
            catch(Exception e){
            	System.out.println(m_name + " MultiplexerMergeTest.setState(InputStream): " + e.toString());
            }
            System.out.println(m_name + " MultiplexerMergeTest.setState(InputStream) - setting stream state = " + new String(m_state));
        }
        
        public void getState(OutputStream os) {
            System.out.println(m_name + " MultiplexerMergeTest.getState(OutputStream) returning stream state = " + new String(m_state));
            try {
                os.write(m_state);
                os.flush();
                os.close();
            } catch (IOException e) {
                System.out.println(m_name + " MultiplexerMergeTest.getState(OutputStream) failed: " + e.toString());
            }
        }
        
        public byte[] getState(String state_id) {
            System.out.println(m_name + " MultiplexerMergeTest.getState(String) - not implemented"); 
            return null;
        }
        
        public void getState(String state_id, OutputStream os) {
            System.out.println(m_name + " MultiplexerMergeTest.getState(String, InputStream) - not implemented"); 
        }
        
        public void setState(String state_id, byte[] state) {
            System.out.println(m_name + " MultiplexerMergeTest.setState(String, byte[]) - not implemented"); 
        }
        
        public void setState(String state_id, InputStream is) {
            System.out.println(m_name + " MultiplexerMergeTest.setState(String, InputStream) - not implemented"); 
        }
    }

    private static byte[] getInputStreamBytes(InputStream is) {
      byte[] b = null;
      if ( is != null ) {
        b = new byte[1024];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          while ( true ) {
            int bytes = is.read( b );
            if ( bytes == -1 ) {
              break;
            }
            baos.write(b, 0, bytes);
          }
        }
        catch ( Exception e ) {
          e.printStackTrace();
        }
        finally {
          try {
              b = baos.toByteArray();
              baos.close();
          }
          catch ( Exception e ) {
            e.printStackTrace();
          }
        }
      }
      return b;
    }

    public static void main(String[] args) {
        String[] testCaseName={MultiplexerMergeTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}

