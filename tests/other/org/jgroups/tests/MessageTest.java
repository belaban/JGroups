package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.util.List;
import org.jgroups.stack.IpAddress;

import java.io.*;
import java.util.Iterator;

/**
 * @author Bela Ban Feb 12, 2004
 * @version $Id: MessageTest.java,v 1.1 2004/02/25 19:25:45 belaban Exp $
 */
public class MessageTest {

    public void start(int num) throws IOException {
        Message msg;
        byte[] buf;
        int NUM=num;
        long start, stop, total;
        double msgs_per_sec, time_per_msg;
        List my_list=new List();

        IpAddress dest=new IpAddress(7500);

        System.out.println("-- starting to create " + NUM + " msgs");
        start=System.currentTimeMillis();
        for(int i=1; i <= NUM; i++) {
            msg=new Message(null, null, "Hello world from message #" + i);
            my_list.add(msg);
        }
        stop=System.currentTimeMillis();
        total=stop-start;
        msgs_per_sec=NUM / (total/1000.0);
        time_per_msg=total / (double)NUM;
        System.out.println("\n-- total time for creating " + NUM +
                " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + " ms)");

        System.out.println("-- starting to serialize " + NUM + " msgs");
        start=System.currentTimeMillis();
        ByteArrayOutputStream output=new ByteArrayOutputStream(1024);
        ObjectOutputStream out=new ObjectOutputStream(output);
        my_list.writeExternal(out);
        out.close();
        buf=output.toByteArray();
        stop=System.currentTimeMillis();

        total=stop-start;
        msgs_per_sec=NUM / (total/1000.0);
        time_per_msg=total / (double)NUM;
        System.out.println("\n-- total time for serializing " + NUM +
                " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + " ms)");



        System.out.println("-- starting to unserialize msgs");
        start=System.currentTimeMillis();
        ByteArrayInputStream input=new ByteArrayInputStream(buf);
        ObjectInputStream in=new ObjectInputStream(input);
        int msgs_read=0;

        List l2=new List();
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
                " msgs = " + total + "ms \n(" + msgs_per_sec + " msgs/sec, time_per_msg=" + time_per_msg + ")");

    }

    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("MessageTest <num>");
            return;
        }

        try {
            new MessageTest().start(Integer.parseInt(args[0]));
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
}
