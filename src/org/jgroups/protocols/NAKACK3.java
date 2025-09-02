package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.Buffer;
import org.jgroups.util.DynamicBuffer;

import java.util.Map;

/**
 * Negative AcKnowledgement layer (NAKs). Messages are assigned a monotonically increasing sequence number (seqno).
 * Receivers deliver messages ordered according to seqno and request retransmission of missing messages.<p>
 * Retransmit requests are usually sent to the original sender of a message, but this can be changed by
 * xmit_from_random_member (send to random member) or use_mcast_xmit_req (send to everyone). Responses can also be sent
 * to everyone instead of the requester by setting use_mcast_xmit to true.
 * @author Bela Ban
 * @since  5.4
 */
public class NAKACK3 extends ReliableMulticast {

    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    protected int     xmit_table_num_rows=100;

    @Property(description="Number of elements of a row of the matrix in the retransmission table; gets rounded to the " +
      "next power of 2 (only for experts). The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",
      writable=false)
    protected int     xmit_table_msgs_per_row=1024;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    protected double  xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false,type=AttributeType.TIME)
    protected long    xmit_table_max_compaction_time=10000;


    public int               getXmitTableNumRows()                 {return xmit_table_num_rows;}
    public ReliableMulticast setXmitTableNumRows(int x)            {this.xmit_table_num_rows=x; return this;}
    public int               getXmitTableMsgsPerRow()              {return xmit_table_msgs_per_row;}
    public ReliableMulticast setXmitTableMsgsPerRow(int x)         {this.xmit_table_msgs_per_row=x; return this;}
    public double            getXmitTableResizeFactor()            {return xmit_table_resize_factor;}
    public ReliableMulticast setXmitTableResizeFactor(double x)    {this.xmit_table_resize_factor=x; return this;}
    public long              getXmitTableMaxCompactionTime()       {return xmit_table_max_compaction_time;}
    public ReliableMulticast setXmitTableMaxCompactionTime(long x) {this.xmit_table_max_compaction_time=x; return this;}


    protected Buffer<Message> createXmitWindow(long initial_seqno) {
        return new DynamicBuffer<>(xmit_table_num_rows, xmit_table_msgs_per_row,
                                   initial_seqno, xmit_table_resize_factor, xmit_table_max_compaction_time);
    }

    @ManagedAttribute(description="Prints the number of rows currently allocated in the matrix. This value will not " +
      "be lower than xmit_table_now_rows")
    public int getXmitTableNumCurrentRows() {
        DynamicBuffer<Message> win=getBuf(local_addr);
        return win != null? win.getNumRows() : 0;
    }

    @ManagedAttribute(description="Number of retransmit table compactions")
    public int getXmitTableNumCompactions() {
        DynamicBuffer<Message> win=getBuf(local_addr);
        return win != null? win.getNumCompactions() : 0;
    }

    @ManagedAttribute(description="Number of retransmit table moves")
    public int getXmitTableNumMoves() {
        DynamicBuffer<Message> win=getBuf(local_addr);
        return win != null? win.getNumMoves() : 0;
    }

    @ManagedAttribute(description="Number of retransmit table resizes")
    public int getXmitTableNumResizes() {
        DynamicBuffer<Message> win=getBuf(local_addr);
        return win != null? win.getNumResizes(): 0;
    }

    @ManagedAttribute(description="Number of retransmit table purges")
    public int getXmitTableNumPurges() {
        DynamicBuffer<Message> win=getBuf(local_addr);
        return win != null? win.getNumPurges(): 0;
    }

    @ManagedOperation(description="Compacts the retransmit buffer")
    public void compact() {
        DynamicBuffer<Message> win=getBuf(local_addr);
        if(win != null)
            win.compact();
    }

    @ManagedOperation(description="Prints the number of rows currently allocated in the matrix for all members. " +
      "This value will not be lower than xmit_table_now_rows")
    public String dumpXmitTablesNumCurrentRows() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Entry> e: xmit_table.entrySet())
            sb.append(String.format("%s: %d\n", e.getKey(), ((DynamicBuffer<Message>)e.getValue().buf()).getNumRows()));
        return sb.toString();
    }

    @Override
    public void init() throws Exception {
        super.init();
        if(reuse_message_batches)
            log.warn("reuse_message_batches is true: this can lead to excessive memory use");
    }
}
