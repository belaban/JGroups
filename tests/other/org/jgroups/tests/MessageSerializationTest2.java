package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.List;
import org.jgroups.util.Buffer;
import org.jgroups.util.ExposedByteArrayOutputStream;

import java.io.*;
import java.util.Enumeration;

/**
 * @author Bela Ban Feb 12, 2004
 * @version $Id: MessageSerializationTest2.java,v 1.6 2004/10/08 11:25:53 belaban Exp $
 */
public class MessageSerializationTest2 {
    Message msg;
    Buffer buf;
    long start, stop, total;
    double msgs_per_sec, time_per_msg;
    List my_list=new List();
    int num=10000;
    ObjectOutputStream out;
    ExposedByteArrayOutputStream output;
    ByteArrayInputStream input;
    ObjectInputStream in;
    DataOutputStream dos;
    DataInputStream dis;
    int msgs_read=0;
    List l2=new List();



    public void start(int num, boolean use_serialization, boolean use_streamable)
            throws IOException, IllegalAccessException, InstantiationException {
        IpAddress dest=new IpAddress("228.8.8.8", 7500);
        IpAddress src=new IpAddress("127.0.0.1", 5555);

        this.num=num;
        System.out.println("-- starting to create " + num + " msgs");
        start=System.currentTimeMillis();
        for(int i=1; i <= num; i++) {
            msg=new Message(dest, src, ("Hello world from message #" +i).getBytes());
            my_list.add(msg);
        }
        stop=System.currentTimeMillis();
        total=stop-start;
        msgs_per_sec=num / (total/1000.0);
        time_per_msg=total / (double)num;
        System.out.println("\n-- total time for creating " + num +
                " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + " ms)");

        if(use_serialization)
            serializeMessage();

        if(use_streamable)
            marshalMessages();
    }


    void serializeMessage() throws IOException {
        System.out.println("-- starting to serialize " + num + " msgs");
        start=System.currentTimeMillis();
        output=new ExposedByteArrayOutputStream(65000);
        out=new ObjectOutputStream(output);
        my_list.writeExternal(out);
        out.close();
        stop=System.currentTimeMillis();
        buf=new Buffer(output.getRawBuffer(), 0, output.size());
        System.out.println("** serialized buffer size=" + buf.getLength() + " bytes");


        total=stop-start;
        msgs_per_sec=num / (total/1000.0);
        time_per_msg=total / (double)num;
        System.out.println("\n-- total time for serializing " + num +
                " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + " ms)");

        System.out.println("-- starting to unserialize msgs");
        start=System.currentTimeMillis();
        ByteArrayInputStream input=new ByteArrayInputStream(buf.getBuf(), buf.getOffset(), buf.getLength());
        ObjectInputStream in=new ObjectInputStream(input);



        try {
            l2.readExternal(in);
        }
        catch(ClassNotFoundException e) {
            e.printStackTrace();
        }
        stop=System.currentTimeMillis();
        total=stop-start;
        msgs_read=l2.size();
        msgs_per_sec=msgs_read / (total/1000.0);
        time_per_msg=total / (double)msgs_read;
        System.out.println("\n-- total time for reading " + msgs_read +
                " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + ')');
        l2.removeAll();

    }

    void marshalMessages() throws IOException, IllegalAccessException, InstantiationException {
        System.out.println("\n\n-- starting to marshal " + num + " msgs (using Streamable)");
        start=System.currentTimeMillis();
        output=new ExposedByteArrayOutputStream(65000);
        dos=new DataOutputStream(output);
        dos.writeInt(my_list.size());
        for(Enumeration en=my_list.elements(); en.hasMoreElements();) {
            Message tmp=(Message)en.nextElement();
            tmp.writeTo(dos);
        }

        dos.close();
        stop=System.currentTimeMillis();
        buf=new Buffer(output.getRawBuffer(), 0, output.size());
        System.out.println("** marshalled buffer size=" + buf.getLength() + " bytes");


        total=stop-start;
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
            tmp=new Message();
            tmp.readFrom(dis);
            l2.add(tmp);
        }

        stop=System.currentTimeMillis();
        total=stop-start;
        msgs_read=l2.size();
        msgs_per_sec=msgs_read / (total/1000.0);
        time_per_msg=total / (double)msgs_read;
        System.out.println("\n-- total time for reading " + msgs_read +
                           " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + ')');
    }

    public static void main(String[] args) {
        int num=10000;
        boolean use_serialization=true, use_streamable=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num")) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-use_serialization")) {
                use_serialization=new Boolean(args[++i]).booleanValue();
                continue;
            }
            if(args[i].equals("-use_streamable")) {
                use_streamable=new Boolean(args[++i]).booleanValue();
                continue;
            }
            help();
            return;
        }

        try {
            new MessageSerializationTest2().start(num, use_serialization, use_streamable);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(IllegalAccessException e) {
            e.printStackTrace();
        }
        catch(InstantiationException e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("MessageSerializationTest2 [-help] [-num <number>] " +
                           "[-use_serialization <true|false>] [-use_streamable <true|false>]");
    }
}
