package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ConcurrentLinkedBlockingQueue;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Alternating Bit Protocol. Use without any UNICASTX protocol. Place it somewhere below NAKACKX. Provides reliable
 * p2p unicasting. Design is in ./doc/design/ABP.txt
 * @author Bela Ban
 * @since  3.6.3
 */
@Experimental @Unsupported
@MBean(description="Alternating Bit Protocol, for reliable p2p unicasts")
public class ABP extends Protocol {

    @Property(description="Interval (in ms) at which a sent msg is resent")
    protected long resend_interval=1000;

    protected final ConcurrentHashMap<Address,Entry> send_map=new ConcurrentHashMap<>(),
                                                     recv_map=new ConcurrentHashMap<>();
    protected TimeScheduler timer;
    protected Address       local_addr;

    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View view=evt.getArg();
                send_map.keySet().retainAll(view.getMembers());
                recv_map.keySet().retainAll(view.getMembers());
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        Address dest;
        if((dest=msg.dest()) == null) // we only handle unicast messages
            return down_prot.down(msg);
        Entry entry=getEntry(send_map, dest);
        entry.send(msg);
        return null;
    }

    public Object up(Message msg) {
        Address dest=msg.dest(), sender=msg.src();
        if(dest == null) // we don't handle multicast messages
            return up_prot.up(msg);

        ABPHeader hdr=msg.getHeader(id);
        if(hdr == null)
            return up_prot.up(msg);
        switch(hdr.type) {
            case data:
                Entry entry=getEntry(recv_map, sender);
                log.trace("%s: <-- %s.msg(%d)", local_addr, sender, hdr.bit);
                if(entry.handleMessage(sender, hdr.bit)) {
                    // deliver
                    return up_prot.up(msg);
                }
                break;
            case ack:
                log.trace("%s: <-- %s.ack(%d)", local_addr, sender, hdr.bit);
                entry=getEntry(send_map, sender);
                entry.handleAck(hdr.bit);
                break;
        }
        return null;
    }

    protected Entry getEntry(ConcurrentMap<Address,Entry> map, Address dest) {
        Entry entry=map.get(dest);
        if(entry == null) {
            Entry existing=map.putIfAbsent(dest, entry=new Entry());
            if(existing != null)
                entry=existing;
        }
        return entry;
    }

    protected enum Type {data, ack};

    protected class Entry implements Runnable {
        protected byte                         bit=0;
        protected final BlockingQueue<Message> send_queue=new ConcurrentLinkedBlockingQueue<>(500);
        protected Thread                       xmit_task;

        protected void send(Message msg) {
            synchronized(send_queue) {
                send_queue.add(msg);
            }
            startTask();
        }

        protected synchronized boolean handleMessage(Address sender, byte msg_bit) {
            boolean retval=false;
            if(this.bit == msg_bit) {
                this.bit^=1;
                retval=true;
            }

            byte ack_bit=(byte)(this.bit ^ 1);
            Message ack=new Message(sender).putHeader(id, new ABPHeader(Type.ack, ack_bit));
            log.trace("%s: --> %s.ack(%d)", local_addr, sender, ack_bit);
            down_prot.down(ack);
            return retval;
        }

        protected synchronized void handleAck(byte ack_bit) {
            if(this.bit == ack_bit) {
                this.bit^=1;
                if(!send_queue.isEmpty())
                    send_queue.remove(0);
            }
        }

        protected synchronized void startTask() {
            if(xmit_task == null || !xmit_task.isAlive()) {
                xmit_task=new Thread(this, "ABP.XmitTask");
                xmit_task.setDaemon(true);
                xmit_task.start();
            }
        }


        public void run() {
            Message msg=null, copy;
            while(true) {
                synchronized(this) {
                    try {
                        msg=send_queue.poll(1000, TimeUnit.MILLISECONDS);
                        if(msg == null) {
                            Util.sleep(1000);
                            continue;
                        }
                    }
                    catch(InterruptedException e) {
                        return;
                    }

                    copy=msg.copy().putHeader(id, new ABPHeader(Type.data, bit));
                }
                log.trace("%s: --> %s.msg(%d). Msg: %s", local_addr, copy.dest(), bit, copy.printHeaders());
                down_prot.down(copy);
            }
        }
    }

    protected static class ABPHeader extends Header {
        protected Type type;
        protected byte bit;  // either 1 or 0

        public ABPHeader() {}

        public ABPHeader(Type type, byte bit) {
            this.type=type;
            this.bit=bit;
        }
        public short getMagicId() {return 87;}
        @Override
        public Supplier<? extends Header> create() {return ABPHeader::new;}

        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE *2;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type.ordinal());
            out.writeByte(bit);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            type=Type.values()[in.readByte()];
            bit=in.readByte();
        }

        @Override
        public String toString() {
            return "ABP (" + bit + ")";
        }
    }
}
