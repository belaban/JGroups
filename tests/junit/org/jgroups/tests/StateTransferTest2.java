package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Iterator;

/**
 * Tests state transfer API (including exception handling)
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class StateTransferTest2 extends ChannelTestBase {


    @DataProvider(name="createChannels")
    protected Iterator<JChannel[]> createChannels() {
        return new MyIterator(new Class[]{STATE_TRANSFER.class, STATE.class,
          STATE_SOCK.class});
    }

   /* @DataProvider(name="createChannels")
    protected Iterator<JChannel[]> createChannels() {
        return new MyIterator(new Class[]{STATE.class});
    }*/



   
    @Test(dataProvider="createChannels")
    public void testSuccessfulStateTransfer(final JChannel c1, final JChannel c2) throws Exception {
        try {
            StateHandler sh1=new StateHandler("Bela", false, false), sh2=new StateHandler(null, false, false);
            c1.setReceiver(sh1);
            c2.setReceiver(sh2);
            c2.getState(null, 0);
            Object state=sh2.getReceivedState();
            System.out.println("state = " + state);
            assert state != null && state.equals("Bela");
        }
        finally {
            Util.close(c2, c1);
        }
    }

    @Test(dataProvider="createChannels")
    public void testUnsuccessfulStateTransferFailureAtStateProvider(final JChannel c1, final JChannel c2) throws Exception {
        try {
            StateHandler sh1=new StateHandler("Bela", true, false), sh2=new StateHandler(null, false, false);
            c1.setReceiver(sh1);
            c2.setReceiver(sh2);
            try {
                c2.getState(null, 0);
                assert false : "we shouldn't get here; getState() should have thrown an exception";
            }
            catch(StateTransferException ex) {
                System.out.println("getState() threw an exception - as expected: " + ex);
            }
            Object state=sh2.getReceivedState();
            System.out.println("state = " + state);
            assert state == null;
        }
        finally {
            Util.close(c2, c1);
        }
    }


    @Test(dataProvider="createChannels")
    public void testUnsuccessfulStateTransferFailureAtStateRequester(final JChannel c1, final JChannel c2) throws Exception {
        StateHandler sh1=new StateHandler("Bela", false, false), sh2=new StateHandler(null, false, true);
        c1.setReceiver(sh1);
        c2.setReceiver(sh2);
        try {
            c2.getState(null, 0);
            assert false : "we shouldn't get here; getState() should have thrown an exception";
        }
        catch(StateTransferException ste) {
            System.out.println("getState() threw an exception - as expected: " + ste);
        }
        finally {
            Util.close(c2, c1);
        }
    }



    protected class MyIterator implements Iterator<JChannel[]> {
        protected final Class[] stream_transfer_prots;
        protected int           index=0;

        public MyIterator(Class[] stream_transfer_prots) {
            this.stream_transfer_prots=stream_transfer_prots;
        }

        public boolean hasNext() {
            return index < stream_transfer_prots.length;
        }

        public JChannel[] next() {
            try {
                Class next_class=stream_transfer_prots[index++];
                System.out.println("State transfer protocol used: " + next_class);
                return createStateProviderAndRequesterChannels(next_class);
            }
            catch(Exception e) {
                throw new RuntimeException("failed creating a new channel", e);
            }
        }

        public void remove() {
        }



        protected JChannel[] createStateProviderAndRequesterChannels(Class state_transfer_class) throws Exception {
            JChannel[] retval=new JChannel[2];
            retval[0]=createChannel(true, 2, "Provider");
            replaceStateTransferProtocolWith(retval[0], state_transfer_class);
            retval[1]=createChannel(retval[0], "Requester");
            for(JChannel ch: retval)
                ch.connect("StateTransferTest2");
            return retval;
        }

        protected void replaceStateTransferProtocolWith(JChannel ch, Class state_transfer_class) throws Exception {
            ProtocolStack stack=ch.getProtocolStack();
            if(stack.findProtocol(state_transfer_class) != null)
                return; // protocol of the right class is already in stack
            Protocol prot=stack.findProtocol(STATE_TRANSFER.class, StreamingStateTransfer.class);
            Protocol new_state_transfer_protcol=(Protocol)state_transfer_class.newInstance();
            if(prot != null) {
                stack.replaceProtocol(prot, new_state_transfer_protcol);
            }
            else { // no state transfer protocol found in stack
                Protocol flush=stack.findProtocol(FLUSH.class);
                if(flush != null)
                    stack.insertProtocol(new_state_transfer_protcol, ProtocolStack.BELOW, FLUSH.class);
                else
                    stack.insertProtocolAtTop(new_state_transfer_protcol);
            }
        }
    }


    protected static class StateHandler extends ReceiverAdapter {
        protected final boolean get_error;
        protected final boolean set_error;
        protected final Object  state_to_send;
        protected       Object  received_state=null;

        public StateHandler(Object state_to_send, boolean get_error, boolean set_error) {
            this.state_to_send=state_to_send;
            this.get_error=get_error;
            this.set_error=set_error;
        }

        public Object getReceivedState() {
            return received_state;
        }


        public void getState(OutputStream ostream) throws Exception {
            if(get_error)
                throw new RuntimeException("[dummy failure] state could not be serialized");
            DataOutputStream out=new DataOutputStream(new BufferedOutputStream(ostream, 200));
            Util.objectToStream(state_to_send, out);
            out.flush();
        }

        public void setState(InputStream istream) throws Exception {
            if(set_error)
                throw new RuntimeException("[dummy failure] state could not be set");
            DataInputStream in=new DataInputStream(istream);
            try {
                received_state=Util.objectFromStream(in);
            }
            finally {
                Util.close(in);
            }
        }
    }

}