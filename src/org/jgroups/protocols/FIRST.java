package org.jgroups.protocols;


import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

@MBean(description="Implementation of FIRST protocol using a sequencer")
public class FIRST extends Protocol {
	
	    protected Address                           local_addr;
	    protected volatile Address                  coord;
	    protected volatile View                     view;
	    protected volatile boolean                  is_coord=false;
	
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
            	log.info("[" + local_addr + "] "+"received request (down)");
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;

                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);
                
                if (!is_coord){
                	log.info("[" + local_addr + "] "+ "sending to coord (down)");
                	msg.setDest(coord);
                    down_prot.down(evt);
                }
                else{
                	log.info("[" + local_addr + "] "+ "sending to non-coord (down) " + view.getMembers().get(2));
                	msg.setDest(view.getMembers().get(2));
                    down_prot.down(evt);               	
                }

                    

                return null;
                
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
        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();

            	log.info("[" + local_addr + "] "+ "receiving msg from (up) " + msg.getSrc() + " " + msg.getObject());
            	log.info("Thank you " + msg.getSrc());
                if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                
            	log.info("[" + local_addr + "]"+ "receiving msg from (up) " + msg.getSrc() + " " + msg.getObject());
            	log.info("Thank you " + msg.getSrc());
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
    
    private void handleTmpView(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        Address new_coord=mbrs.get(0);
        if(!new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord))
            handleViewChange(v);
    }



    /* --------------------------------- Private Methods ----------------------------------- */

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;


        Address new_leader=Util.getCoordinator(v);
        boolean leader_changed=coord == null || !coord.equals(new_leader);
        if(leader_changed) {
            coord=new_leader;
        	//isLeader = true;      	
        }
        
        is_coord=local_addr != null && local_addr.equals(coord);

       
    }
    
    public static class MyFirstHeader extends Header {
        protected static final byte FORWARD       = 1;
        protected static final byte FLUSH         = 2;
        protected static final byte BCAST         = 3;
        protected static final byte WRAPPED_BCAST = 4;

        protected byte    type=-1;
        protected long    seqno=-1;
        protected boolean flush_ack;

        public MyFirstHeader() {
        }

        public MyFirstHeader(byte type) {
            this.type=type;
        }

        public MyFirstHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }

        public long getSeqno() {
            return seqno;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(flush_ack)
                sb.append(" (flush_ack)");
            return sb.toString();
        }

        protected final String printType() {
            switch(type) {
                case FORWARD:        return "FORWARD";
                case FLUSH:          return "FLUSH";
                case BCAST:          return "BCAST";
                case WRAPPED_BCAST:  return "WRAPPED_BCAST";
                default:             return "n/a";
            }
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            out.writeBoolean(flush_ack);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            flush_ack=in.readBoolean();
        }

        public int size() {
            return Global.BYTE_SIZE + Bits.size(seqno) + Global.BYTE_SIZE; // type + seqno + flush_ack
        }

    }

}
