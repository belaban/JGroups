package org.jgroups.tests.perf;

import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;

/**
 * @author Bela Ban
 * @version $Id: MemberInfo.java,v 1.10 2008/08/01 09:12:49 belaban Exp $
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
        StringBuilder sb=new StringBuilder();
        double msgs_sec, throughput=0;
        long total_time=stop-start;
        double loss_rate=0;
        long missing_msgs=num_msgs_expected - num_msgs_received;
        msgs_sec=num_msgs_received / (total_time/1000.0);
        throughput=total_bytes_received / (total_time / 1000.0);
        loss_rate=missing_msgs >= num_msgs_expected? 100.0 : (100.0 / num_msgs_expected) * missing_msgs;
        sb.append("num_msgs_expected=").append(num_msgs_expected).append(", num_msgs_received=");
        sb.append(num_msgs_received);
        sb.append(" (loss rate=").append(loss_rate).append("%)");
            sb.append(", received=").append(Util.printBytes(total_bytes_received));
        sb.append(", time=").append(f.format(total_time)).append("ms");
        sb.append(", msgs/sec=").append(f.format(msgs_sec));
        sb.append(", throughput=").append(Util.printBytes(throughput));
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
