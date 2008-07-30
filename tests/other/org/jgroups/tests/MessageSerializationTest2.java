package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.ExposedByteArrayOutputStream;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Bela Ban Feb 12, 2004
 * @version $Id: MessageSerializationTest2.java,v 1.15.2.1 2008/07/30 12:04:50 belaban Exp $
 */
public class MessageSerializationTest2 {
    Message msg;
    Buffer buf;
    long start, stop, total;
    double msgs_per_sec, time_per_msg;
    List<Message> my_list=new LinkedList<Message>();
    int num=50000;
    ObjectOutputStream out;
    ExposedByteArrayOutputStream output;
    ByteArrayInputStream input;
    ObjectInputStream in;
    DataOutputStream dos;
    DataInputStream dis;
    int msgs_read=0;
    List<Message> l2=new LinkedList<Message>();



    public void start(int num, boolean use_additional_data, boolean add_headers) throws Exception {
        IpAddress dest=new IpAddress("228.8.8.8", 7500);
        IpAddress src=new IpAddress("127.0.0.1", 5555);
        if(use_additional_data)
            src.setAdditionalData("bela".getBytes());

        ClassConfigurator.getInstance(true);

        this.num=num;
        System.out.println("-- starting to create " + num + " msgs");
        start=System.currentTimeMillis();
        for(int i=1; i <= num; i++) {
            msg=new Message(dest, src, ("Hello world from message #" +i).getBytes());
            if(add_headers) {
                addHeaders(msg);
            }
            my_list.add(msg);
        }
        stop=System.currentTimeMillis();
        total=stop-start;
        msgs_per_sec=num / (total/1000.0);
        time_per_msg=total / (double)num;
        System.out.println("\n-- total time for creating " + num +
                " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + " ms)");

        List<Long> l_ser=null, l_stream=null;

            l_stream=marshalMessages();
    }


     static void addHeaders(Message msg) {
        msg.putHeader("UDP", new UdpHeader("MyGroup"));
        msg.putHeader("PING", new PingHeader(PingHeader.GET_MBRS_REQ, null));
        msg.putHeader("FD_SOCK", new FD_SOCK.FdHeader());
        msg.putHeader("VERIFY_SUSPECT", new VERIFY_SUSPECT.VerifyHeader());
        msg.putHeader("STABLE", new org.jgroups.protocols.pbcast.STABLE.StableHeader());
        msg.putHeader("NAKACK", new org.jgroups.protocols.pbcast.NakAckHeader());
        msg.putHeader("UNICAST", new UNICAST.UnicastHeader());
        msg.putHeader("FRAG", new FragHeader());
        msg.putHeader("GMS", new org.jgroups.protocols.pbcast.GMS.GmsHeader());
    }

    private static void printDiffs(List<Long> l_ser, List<Long> l_stream) {
        long size_ser, size_stream;
        long write_ser, write_stream, read_ser, read_stream;

        size_ser=(l_ser.get(0));
        size_stream=l_stream.get(0);
        write_ser=l_ser.get(1).longValue();
        read_ser=l_ser.get(2).longValue();
        write_stream=l_stream.get(1).longValue();
        read_stream=(l_stream.get(2)).longValue();
        System.out.println("\n\nserialized size=" + size_ser + ", streamable size=" + size_stream +
                           ", streamable is " + (100.0 / size_stream * size_ser -100) + " percent smaller");
        System.out.println("serialized write=" + write_ser + ", streamable write=" + write_stream +
                           ", streamable write is " + (100.0 / write_stream * write_ser -100) + " percent faster");
        System.out.println("serialized read=" + read_ser + ", streamable read=" + read_stream +
                           ", streamable read is " + (100.0 / read_stream * read_ser -100) + " percent faster");
    }




    LinkedList<Long> marshalMessages() throws IOException, IllegalAccessException, InstantiationException {
        LinkedList<Long> retval=new LinkedList<Long>();
        System.out.println("\n\n-- starting to marshal " + num + " msgs (using Streamable)");
        start=System.currentTimeMillis();
        output=new ExposedByteArrayOutputStream(65000);
        dos=new DataOutputStream(output);
        dos.writeInt(my_list.size());
        for(Message tmp: my_list) {
            tmp.writeTo(dos);
        }

        dos.close();
        stop=System.currentTimeMillis();
        buf=new Buffer(output.getRawBuffer(), 0, output.size());
        System.out.println("** marshalled buffer size=" + buf.getLength() + " bytes");
        retval.add(new Long(buf.getLength()));

        total=stop-start;
        retval.add(new Long(total));
        msgs_per_sec=num / (total/1000.0);
        time_per_msg=total / (double)num;
        System.out.println("\n-- total time for marshaling " + num +
                           " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + " ms)");

        System.out.println("-- starting to unmarshal msgs (using Streamable)");
        start=System.currentTimeMillis();
        input=new ByteArrayInputStream(buf.getBuf(), buf.getOffset(), buf.getLength());
        dis=new DataInputStream(input);
        msgs_read=0;

        int b=dis.readInt();
        Message tmp;
        for(int i=0; i < b; i++) {
            tmp=new Message(false);
            tmp.readFrom(dis);
            l2.add(tmp);
        }

        stop=System.currentTimeMillis();
        total=stop-start;
        retval.add(new Long(total));
        msgs_read=l2.size();
        msgs_per_sec=msgs_read / (total/1000.0);
        time_per_msg=total / (double)msgs_read;
        System.out.println("\n-- total time for reading " + msgs_read +
                           " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + ')');
        return retval;
    }



    public static void main(String[] args) {
        int num=50000;
        boolean use_additional_data=false, add_headers=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num")) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-use_additional_data")) {
                use_additional_data=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-add_headers")) {
                add_headers=Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }

        try {
            new MessageSerializationTest2().start(num, use_additional_data, add_headers);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("MessageSerializationTest2 [-help] [-num <number>] " +
                           "[-use_additional_data <true|false>] [-add_headers <true|false>]");
    }
}
