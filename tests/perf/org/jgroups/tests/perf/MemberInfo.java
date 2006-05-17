package org.jgroups.tests.perf;

import org.jgroups.util.Streamable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: MemberInfo.java,v 1.7 2006/05/17 07:40:30 belaban Exp $
 */
public class MemberInfo implements Streamable {
    public  long start=0;
    public  long stop=0;
    public  long num_msgs_expected=0;
    public  long num_msgs_received=0;
    boolean done=false;
    long    total_bytes_received=0;

    static  NumberFormat f;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }

    public MemberInfo() {
    }

    public MemberInfo(long num_msgs_expected) {
        this.num_msgs_expected=num_msgs_expected;
    }

    public double getMessageSec() {
        long total_time=stop-start;
        return num_msgs_received / (total_time/1000.0);
    }


    public String toString() {
        StringBuffer sb=new StringBuffer();
        double msgs_sec, throughput_kb=0, throughput_mb=0, kb_received=0, mb_received=0;
        long total_time=stop-start;
        double loss_rate=0;
        long missing_msgs=num_msgs_expected - num_msgs_received;
        kb_received=total_bytes_received/1000.0;
        if(kb_received >= 1000)
            mb_received=kb_received / 1000.0;
        msgs_sec=num_msgs_received / (total_time/1000.0);
        throughput_kb=kb_received / (total_time / 1000.0);
        if(throughput_kb >= 1000)
            throughput_mb=throughput_kb / 1000.0;
        loss_rate=missing_msgs >= num_msgs_expected? 100.0 : (100.0 / num_msgs_expected) * missing_msgs;
        sb.append("num_msgs_expected=").append(num_msgs_expected).append(", num_msgs_received=");
        sb.append(num_msgs_received);
        sb.append(" (loss rate=").append(loss_rate).append("%)");
        if(mb_received > 0)
            sb.append(", received=").append(f.format(mb_received)).append("MB");
        else
            sb.append(", received=").append(f.format(kb_received)).append("KB");
        sb.append(", time=").append(f.format(total_time)).append("ms");
        sb.append(", msgs/sec=").append(f.format(msgs_sec));
        if(throughput_mb > 0)
            sb.append(", throughput=").append(f.format(throughput_mb)).append("MB/sec");
        else
            sb.append(", throughput=").append(f.format(throughput_kb)).append("KB/sec");
        return sb.toString();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(start);
        out.writeLong(stop);
        out.writeLong(num_msgs_expected);
        out.writeLong(num_msgs_received);
        out.writeBoolean(done);
        out.writeLong(total_bytes_received);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        start=in.readLong();
        stop=in.readLong();
        num_msgs_expected=in.readLong();
        num_msgs_received=in.readLong();
        done=in.readBoolean();
        total_bytes_received=in.readLong();

    }
}
