package org.jgroups.protocols.jzookeeper;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;





/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */
@Unsupported
@MBean(description="Sample protocol")
public class TestProtocol extends Protocol {
	
	

	protected final AtomicLong        zxid=new AtomicLong(0);
    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0);

    private final LinkedBlockingQueue<Message> queuedMessages =
	        new LinkedBlockingQueue<Message>();
    final List<Address> members=new ArrayList<Address>();


    /**
     * Just remove if you don't need to reset any state
     */
    public static void reset() {
    }

    
    public void start() throws Exception {
        super.start();
	    log.setLevel("trace");
	    
    }

    public void stop() {
       
        super.stop();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
            	log.info("[" + local_addr + "] "+"received request (down TestProtocol)");
            	log.info("[" + local_addr + "] "+ "print view (down) " +view.getMembers());

                Message msg=(Message)evt.getArg();
//                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB)){
//                	log.info("[" + local_addr + "] "+ "going to break ");
//                    break;
//
//                }

                if(msg.getSrc() == null){
                	log.info("[" + local_addr + "] "+"inside if(msg.getSrc() == null) (down)");
                       msg.setSrc(local_addr);
                }
                
               
                try {
                    log.info("[" + local_addr + "] "+"invloking forwardToCoord method  (down)"+msg);
                    forwardToCoord(msg);
                }
                catch(Exception ex) {
                    log.error("failed sending message", ex);
                }
                
                return null; // don't pass down

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }




    public Object up(Event evt) {
        Message msg;
        TestHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                hdr=(TestHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break; // pass up
                log.info("[" + local_addr + "] "+ " received message from (up) " + msg.getSrc() + " type "+ hdr.type);

                switch(hdr.type) {
                    case TestHeader.FORWARDTOFOLLOER:
                    	
                    	
                		try {
                    		log.info("[" + local_addr + "] "+"received FORWARDTOFOLLOER");
                			queuedMessages.add(msg);
                		}
                		catch(Exception ex) {
                			log.error("failed forwarding message to " + msg.getDest(), ex);
                		}
                		break;
                    case TestHeader.FORWARDTOLEADER:

                		try {
                    		log.info("[" + local_addr + "] "+"received FORWARDTOLEADER");
                			queuedMessages.add(msg);
                		}
                		catch(Exception ex) {
                			log.error("failed forwarding message to " + msg.getDest(), ex);
                		}
                		break;
                    
            }                
                return null;
            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange((View)evt.getArg());
                return retval;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;
        }

        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            batch.remove(msg);

            // simplistic implementation
            try {
                up(new Event(Event.MSG, msg));
            }
            catch(Throwable t) {
                log.error("failed passing up message", t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    
    protected void forwardToCoord(Message msg) {
        //if(is_coord) {
        	//next_seqno=seqno.incrementAndGet();
            log.info("[ " + local_addr + "] "+"recieved msg (forwardToCoord) (if (is_coord) "+msg);
            forward(msg);
           
        }

//       
    protected void forward(final Message msg) {
        Address target=coord;
        if(target == null)
            return;
        
        if(is_coord){
        byte type=TestHeader.FORWARDTOFOLLOER;
        log.info("[" + local_addr + "] "+"recieved msg (forward) FORWARDTOFOLLOER "+msg + " type " + type);

        try {
            TestHeader hdr=new TestHeader(type);
            Message forward_msg=new Message(null, Util.objectToByteBuffer(msg)).putHeader(this.id,hdr);
            down_prot.down(new Event(Event.MSG, forward_msg));
        }
        catch(Exception ex) {
            log.error("failed forwarding message to " + msg.getDest(), ex);
        }
    }
        else{
        	  byte type=TestHeader.FORWARDTOLEADER;
              log.info("[" + local_addr + "] "+"recieved msg (forward FORWARDTOLEADER) "+msg + " type " + type);

              try {
                  TestHeader hdr=new TestHeader(type);
                  Message forward_msg=new Message(null, Util.objectToByteBuffer(msg)).putHeader(this.id,hdr);
                  down_prot.down(new Event(Event.MSG, forward_msg));
              }
              catch(Exception ex) {
                  log.error("failed forwarding message to " + msg.getDest(), ex);
              }
        	
        }
    }
    /* --------------------------------- Private Methods ----------------------------------- */

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;

        coord = mbrs.get(0);
        if (local_addr.equals(coord)){
        	is_coord = true;
        }
       
        Address existing_coord=coord, new_coord=mbrs.get(0);
        boolean coord_changed=existing_coord == null || !existing_coord.equals(new_coord);
        if(coord_changed && new_coord != null) {
        }
    }

    
 // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        Address new_coord=mbrs.get(0);
        if(!new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord))
            handleViewChange(v);
    }
    public static class TestHeader extends Header {
    	
    	 private static final byte FORWARDTOLEADER       = 1;
         private static final byte FORWARDTOFOLLOER      = 2;
         
         

        protected byte    type=-1;
        
        public TestHeader() {
        }

        public TestHeader(byte type) {
            this.type=type;
        }

        public TestHeader(byte type, long seqno) {
            this(type);
        }

       

        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            
            return sb.toString();
        }

        protected final String printType() {
        	
        	switch(type) {
            case FORWARDTOLEADER:        return "FORWARDTOLEADER";
            case FORWARDTOFOLLOER:       return "FORWARDTOFOLLOER";
            default:             return "n/a";
        }
           
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            
        }

        public int size() {
            return Global.BYTE_SIZE  + Global.BYTE_SIZE; // type + seqno + flush_ack
        }

    }

}
