// $Id: Util.java,v 1.25 2004/11/29 09:29:32 belaban Exp $

package org.jgroups.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.IpAddress;

import java.io.*;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;


/**
 * Collection of various utility routines that can not be assigned to other classes.
 */
public class Util {
    private static final Object mutex=new Object();
    private static final ByteArrayOutputStream out_stream=new ByteArrayOutputStream(512);

    protected static final Log log=LogFactory.getLog(Util.class);

    // constants
    public static final int MAX_PORT=65535; // highest port allocatable
    public static final String DIAG_GROUP="DIAG_GROUP-BELA-322649"; // unique


    /**
     * Creates an object from a byte buffer
     */
    public static Object objectFromByteBuffer(byte[] buffer) throws Exception {
        synchronized(mutex) {
            if(buffer == null) return null;
            Object retval=null;
            ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer);
            ObjectInputStream in=new ContextObjectInputStream(in_stream); // changed Nov 29 2004 (bela)
            retval=in.readObject();
            in.close();
            if(retval == null)
                return null;
            return retval;
        }
    }

    /**
     * Serializes an object into a byte buffer.
     * The object has to implement interface Serializable or Externalizable
     */
    public static byte[] objectToByteBuffer(Object obj) throws Exception {
        byte[] result=null;
        synchronized(out_stream) {
            out_stream.reset();
            ObjectOutputStream out=new ObjectOutputStream(out_stream);
            out.writeObject(obj);
            result=out_stream.toByteArray();
            out.close();
        }
        return result;
    }


    public static void writeAddress(Address addr, DataOutputStream out) throws IOException {
        if(addr == null) {
            out.write(0);
            return;
        }

        out.write(1);
        if(addr instanceof IpAddress) {
            // regular case, we don't need to include class information about the type of Address, e.g. JmsAddress
            out.write(1);
            addr.writeTo(out);
        }
        else {
            out.write(0);
            writeOtherAddress(addr, out);
        }
    }




    private static void writeOtherAddress(Address addr, DataOutputStream out) throws IOException {
        ClassConfigurator conf=null;
        try {conf=ClassConfigurator.getInstance(false);} catch(Exception e) {}
        int magic_number=conf != null? conf.getMagicNumber(addr.getClass()) : -1;

        // write the class info
        if(magic_number == -1) {
            out.write(0);
            out.writeUTF(addr.getClass().getName());
        }
        else {
            out.write(1);
            out.writeInt(magic_number);
        }

        // write the data itself
        addr.writeTo(out);
    }

    public static Address readAddress(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        Address addr=null;
        int b=in.read();
        if(b == 0)
            return null;
        b=in.read();
        if(b == 1) {
            addr=new IpAddress();
            addr.readFrom(in);
        }
        else {
            addr=readOtherAddress(in);
        }
        return addr;
    }

    private static Address readOtherAddress(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        ClassConfigurator conf=null;
        try {conf=ClassConfigurator.getInstance(false);} catch(Exception e) {}
        int b=in.read();
        int magic_number;
        String classname;
        Class cl=null;
        Address addr;
        if(b == 1) {
            magic_number=in.readInt();
            cl=conf.get(magic_number);
        }
        else {
            classname=in.readUTF();
            cl=conf.get(classname);
        }
        addr=(Address)cl.newInstance();
        addr.readFrom(in);
        return addr;
    }


    public static void writeStreamable(Streamable obj, DataOutputStream out) throws IOException {
        if(obj == null) {
            out.write(0);
            return;
        }
        out.write(1);
        obj.writeTo(out);
    }


    public static Streamable readStreamable(Class clazz, DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        Streamable retval=null;
        int b=in.read();
        if(b == 0)
            return null;
        retval=(Streamable)clazz.newInstance();
        retval.readFrom(in);
        return retval;
    }


    public static void writeGenericStreamable(Streamable obj, DataOutputStream out) throws IOException {
        int magic_number;
        String classname;

        if(obj == null) {
            out.write(0);
            return;
        }

        try {
            out.write(1);
            magic_number=ClassConfigurator.getInstance(false).getMagicNumber(obj.getClass());
            // write the magic number or the class name
            if(magic_number == -1) {
                out.write(0);
                classname=obj.getClass().getName();
                out.writeUTF(classname);
            }
            else {
                out.write(1);
                out.writeInt(magic_number);
            }

            // write the contents
            obj.writeTo(out);
        }
        catch(ChannelException e) {
            throw new IOException("failed writing object of type " + obj.getClass() + " to stream: " + e.toString());
        }
    }



    public static Streamable readGenericStreamable(DataInputStream in) throws IOException {
        Streamable retval=null;
        int b=in.read();
        if(b == 0)
            return null;

        int use_magic_number=in.read(), magic_number;
        String classname;
        Class clazz;

        try {
            if(use_magic_number == 1) {
                magic_number=in.readInt();
                clazz=ClassConfigurator.getInstance(false).get(magic_number);
            }
            else {
                classname=in.readUTF();
                clazz=ClassConfigurator.getInstance(false).get(classname);
            }

            retval=(Streamable)clazz.newInstance();
            retval.readFrom(in);
            return retval;
        }
        catch(Exception ex) {
            throw new IOException("failed reading object: " + ex.toString());
        }
    }


    public static void writeString(String s, DataOutputStream out) throws IOException {
        if(s != null) {
            out.write(1);
            out.writeUTF(s);
        }
        else {
            out.write(0);
        }
    }

    public static String readString(DataInputStream in) throws IOException {
        int b=in.read();
        if(b == 1)
            return in.readUTF();
        return null;
    }

    public static void writeByteBuffer(byte[] buf, DataOutputStream out) throws IOException {
        if(buf != null) {
            out.write(1);
            out.writeInt(buf.length);
            out.write(buf, 0, buf.length);
        }
        else {
            out.write(0);
        }
    }

    public static byte[] readByteBuffer(DataInputStream in) throws IOException {
        int b=in.read();
        if(b == 1) {
            b=in.readInt();
            byte[] buf=new byte[b];
            in.read(buf, 0, buf.length);
            return buf;
        }
        return null;
    }



    public static boolean match(Object obj1, Object obj2) {
        if(obj1 == null && obj2 == null)
            return true;
        if(obj1 != null)
            return obj1.equals(obj2);
        else
            return obj2.equals(obj1);
    }


    public static boolean match(long[] a1, long[] a2) {
        if(a1 == null && a2 == null)
            return true;
        if(a1 == null || a2 == null)
            return false;

        if(a1 == a2) // identity
            return true;

        // at this point, a1 != null and a2 != null
        if(a1.length != a2.length)
            return false;

        for(int i=0; i < a1.length; i++) {
            if(a1[i] != a2[i])
                return false;
        }
        return true;
    }

    /** Sleep for timeout msecs. Returns when timeout has elapsed or thread was interrupted */
    public static void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        }
        catch(Exception e) {
        }
    }


    /**
     * On most UNIX systems, the minimum sleep time is 10-20ms. Even if we specify sleep(1), the thread will
     * sleep for at least 10-20ms. On Windows, sleep() seems to be implemented as a busy sleep, that is the
     * thread never relinquishes control and therefore the sleep(x) is exactly x ms long.
     */
    public static void sleep(long msecs, boolean busy_sleep) {
        if(!busy_sleep) {
            sleep(msecs);
            return;
        }

        long start=System.currentTimeMillis();
        long stop=start + msecs;

        while(stop > start) {
            start=System.currentTimeMillis();
        }
    }


    /** Returns a random value in the range [1 - range] */
    public static long random(long range) {
        return (long)((Math.random() * 100000) % range) + 1;
    }


    /** Sleeps between 1 and timeout milliseconds, chosen randomly. Timeout must be > 1 */
    public static void sleepRandom(long timeout) {
        if(timeout <= 0) {
            log.error("timeout must be > 0 !");
            return;
        }

        long r=(int)((Math.random() * 100000) % timeout) + 1;
        sleep(r);
    }


    /**
     Tosses a coin weighted with probability and returns true or false. Example: if probability=0.8,
     chances are that in 80% of all cases, true will be returned and false in 20%.
     */
    public static boolean tossWeightedCoin(double probability) {
        long r=random(100);
        long cutoff=(long)(probability * 100);
        if(r < cutoff)
            return true;
        else
            return false;
    }


    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        }
        catch(Exception ex) {
        }
        return "localhost";
    }


    public static void dumpStack(boolean exit) {
        try {
            throw new Exception("Dumping stack:");
        }
        catch(Exception e) {
            e.printStackTrace();
            if(exit)
                System.exit(0);
        }
    }


    /**
     * Use with caution: lots of overhead
     */
    public static String printStackTrace(Throwable t) {
        StringWriter s=new StringWriter();
        PrintWriter p=new PrintWriter(s);
        t.printStackTrace(p);
        return s.toString();
    }

    public static String getStackTrace(Throwable t) {
        return printStackTrace(t);
    }

    /**
     * Use with caution: lots of overhead
     */
    public static String printStackTrace() {
        try {
            throw new Exception("Dumping stack:");
        }
        catch(Throwable t) {
            StringWriter s=new StringWriter();
            PrintWriter p=new PrintWriter(s);
            t.printStackTrace(p);
            return s.toString();
        }
    }

    public static String print(Throwable t) {
        return printStackTrace(t);
    }


    public static void crash() {
        System.exit(-1);
    }


    public static String printEvent(Event evt) {
        Message msg;

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            if(msg != null) {
                if(msg.getLength() > 0)
                    return printMessage(msg);
                else
                    return msg.printObjectHeaders();
            }
        }
        return evt.toString();
    }


    /** Tries to read an object from the message's buffer and prints it */
    public static String printMessage(Message msg) {
        if(msg == null)
            return "";
        if(msg.getLength() == 0)
            return null;

        try {
            return msg.getObject().toString();
        }
        catch(Exception e) {  // it is not an object
            return "";
        }

    }


    /** Tries to read a <code>MethodCall</code> object from the message's buffer and prints it.
     Returns empty string if object is not a method call */
    public static String printMethodCall(Message msg) {
        Object obj;
        if(msg == null)
            return "";
        if(msg.getLength() == 0)
            return "";

        try {
            obj=msg.getObject();
            return obj.toString();
        }
        catch(Exception e) {  // it is not an object
            return "";
        }

    }


    public static void printThreads() {
        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        System.out.println("------- Threads -------");
        for(int i=0; i < threads.length; i++) {
            System.out.println("#" + i + ": " + threads[i]);
        }
        System.out.println("------- Threads -------\n");
    }


    public static String activeThreads() {
        StringBuffer sb=new StringBuffer();
        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        sb.append("------- Threads -------\n");
        for(int i=0; i < threads.length; i++) {
            sb.append("#" + i + ": " + threads[i] + '\n');
        }
        sb.append("------- Threads -------\n");
        return sb.toString();
    }


    /**
     Fragments a byte buffer into smaller fragments of (max.) frag_size.
     Example: a byte buffer of 1024 bytes and a frag_size of 248 gives 4 fragments
     of 248 bytes each and 1 fragment of 32 bytes.
     @return An array of byte buffers (<code>byte[]</code>).
     */
    public static byte[][] fragmentBuffer(byte[] buf, int frag_size) {
        byte[] retval[];
        long total_size=buf.length;
        int accumulated_size=0;
        byte[] fragment;
        int tmp_size=0;
        int num_frags;
        int index=0;

        num_frags=buf.length % frag_size == 0 ? buf.length / frag_size : buf.length / frag_size + 1;
        retval=new byte[num_frags][];

        while(accumulated_size < total_size) {
            if(accumulated_size + frag_size <= total_size)
                tmp_size=frag_size;
            else
                tmp_size=(int)(total_size - accumulated_size);
            fragment=new byte[tmp_size];
            System.arraycopy(buf, accumulated_size, fragment, 0, tmp_size);
            retval[index++]=fragment;
            accumulated_size+=tmp_size;
        }
        return retval;
    }


    /**
     * Given a buffer and a fragmentation size, compute a list of fragmentation offset/length pairs, and
     * return them in a list. Example:<br/>
     * Buffer is 10 bytes, frag_size is 4 bytes. Return value will be ({0,4}, {4,4}, {8,2}).
     * This is a total of 3 fragments: the first fragment starts at 0, and has a length of 4 bytes, the second fragment
     * starts at offset 4 and has a length of 4 bytes, and the last fragment starts at offset 8 and has a length
     * of 2 bytes.
     * @param frag_size
     * @return List. A List<Range> of offset/length pairs
     */
    public static java.util.List computeFragOffsets(int offset, int length, int frag_size) {
        java.util.List   retval=new ArrayList();
        long   total_size=length + offset;
        int    index=offset;
        int    tmp_size=0;
        Range  r;

        while(index < total_size) {
            if(index + frag_size <= total_size)
                tmp_size=frag_size;
            else
                tmp_size=(int)(total_size - index);
            r=new Range(index, tmp_size);
            retval.add(r);
            index+=tmp_size;
        }
        return retval;
    }

    public static java.util.List computeFragOffsets(byte[] buf, int frag_size) {
        return computeFragOffsets(0, buf.length, frag_size);
    }

    /**
     Concatenates smaller fragments into entire buffers.
     @param fragments An array of byte buffers (<code>byte[]</code>)
     @return A byte buffer
     */
    public static byte[] defragmentBuffer(byte[] fragments[]) {
        int total_length=0;
        byte[] ret;
        int index=0;

        if(fragments == null) return null;
        for(int i=0; i < fragments.length; i++) {
            if(fragments[i] == null) continue;
            total_length+=fragments[i].length;
        }
        ret=new byte[total_length];
        for(int i=0; i < fragments.length; i++) {
            if(fragments[i] == null) continue;
            System.arraycopy(fragments[i], 0, ret, index, fragments[i].length);
            index+=fragments[i].length;
        }
        return ret;
    }


    public static void printFragments(byte[] frags[]) {
        for(int i=0; i < frags.length; i++)
            System.out.println('\'' + new String(frags[i]) + '\'');
    }



//      /**
//         Peeks for view on the channel until n views have been received or timeout has elapsed.
//         Used to determine the view in which we want to start work. Usually, we start as only
//         member in our own view (1st view) and the next view (2nd view) will be the full view
//         of all members, or a timeout if we're the first member. If a non-view (a message or
//         block) is received, the method returns immediately.
//         @param channel The channel used to peek for views. Has to be operational.
//         @param number_of_views The number of views to wait for. 2 is a good number to ensure that,
//                if there are other members, we start working with them included in our view.
//         @param timeout Number of milliseconds to wait until view is forced to return. A value
//                of <= 0 means wait forever.
//       */
//      public static View peekViews(Channel channel, int number_of_views, long timeout) {
//  	View     retval=null;
//  	Object   obj=null;
//  	int      num=0;
//  	long     start_time=System.currentTimeMillis();

//  	if(timeout <= 0) {
//  	    while(true) {
//  		try {
//  		    obj=channel.peek(0);
//  		    if(obj == null || !(obj instanceof View))
//  			break;
//  		    else {
//  			retval=(View)channel.receive(0);
//  			num++;
//  			if(num >= number_of_views)
//  			    break;
//  		    }
//  		}
//  		catch(Exception ex) {
//  		    break;
//  		}
//  	    }
//  	}
//  	else {
//  	    while(timeout > 0) {
//  		try {
//  		    obj=channel.peek(timeout);
//  		    if(obj == null || !(obj instanceof View))
//  			break;
//  		    else {
//  			retval=(View)channel.receive(timeout);
//  			num++;
//  			if(num >= number_of_views)
//  			    break;
//  		    }
//  		}
//  		catch(Exception ex) {
//  		    break;
//  		}
//  		timeout=timeout - (System.currentTimeMillis() - start_time);
//  	    }
//  	}

//  	return retval;
//      }




    public static String array2String(long[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i] + " ");
        }

        ret.append(']');
        return ret.toString();
    }

    public static String array2String(int[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i] + " ");
        }

        ret.append(']');
        return ret.toString();
    }

    public static String array2String(boolean[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i] + " ");
        }
        ret.append(']');
        return ret.toString();
    }


    /**
     * Selects a random subset of members according to subset_percentage and returns them.
     * Picks no member twice from the same membership. If the percentage is smaller than 1 -> picks 1 member.
     */
    public static Vector pickSubset(Vector members, double subset_percentage) {
        Vector ret=new Vector(), tmp_mbrs;
        int num_mbrs=members.size(), subset_size, index;

        if(num_mbrs == 0) return ret;
        subset_size=(int)Math.ceil(num_mbrs * subset_percentage);

        tmp_mbrs=(Vector)members.clone();

        for(int i=subset_size; i > 0 && tmp_mbrs.size() > 0; i--) {
            index=(int)((Math.random() * num_mbrs) % tmp_mbrs.size());
            ret.addElement(tmp_mbrs.elementAt(index));
            tmp_mbrs.removeElementAt(index);
        }

        return ret;
    }


    /**
     * Returns all members that left between 2 views. All members that are element of old_mbrs but not element of
     * new_mbrs are returned.
     */
    public static Vector determineLeftMembers(Vector old_mbrs, Vector new_mbrs) {
        Vector retval=new Vector();
        Object mbr;

        if(old_mbrs == null || new_mbrs == null)
            return retval;

        for(int i=0; i < old_mbrs.size(); i++) {
            mbr=old_mbrs.elementAt(i);
            if(!new_mbrs.contains(mbr))
                retval.addElement(mbr);
        }

        return retval;
    }


    public static String printMembers(Vector v) {
        StringBuffer sb=new StringBuffer("(");
        boolean first=true;
        Object el;

        if(v != null) {
            for(int i=0; i < v.size(); i++) {
                if(!first)
                    sb.append(", ");
                else
                    first=false;
                el=v.elementAt(i);
                if(el instanceof Address)
                    sb.append(el);
                else
                    sb.append(el);
            }
        }
        sb.append(')');
        return sb.toString();
    }


    /**
     Makes sure that we detect when a peer connection is in the closed state (not closed while we send data,
     but before we send data). 2 writes ensure that, if the peer closed the connection, the first write
     will send the peer from FIN to RST state, and the second will cause a signal (IOException).
     */
    public static void doubleWrite(byte[] buf, OutputStream out) throws Exception {
        if(buf.length > 1) {
            out.write(buf, 0, 1);
            out.write(buf, 1, buf.length - 1);
        }
        else {
            out.write(buf, 0, 0);
            out.write(buf);
        }
    }


    public static long sizeOf(String classname) {
        Object inst;
        byte[] data;

        try {
// use thread context class loader
//
            ClassLoader loader=Thread.currentThread().getContextClassLoader();
            inst=loader.loadClass(classname).newInstance();
            data=Util.objectToByteBuffer(inst);
            return data.length;
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error("exception=" + ex);
            return 0;
        }
    }


    public static long sizeOf(Object inst) {
        byte[] data;

        try {
            data=Util.objectToByteBuffer(inst);
            return data.length;
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error("exception+" + ex);
            return 0;
        }
    }


    /** Checks whether 2 Addresses are on the same host */
    public static boolean sameHost(Address one, Address two) {
        InetAddress a, b;
        String host_a, host_b;

        if(one == null || two == null) return false;
        if(!(one instanceof IpAddress) || !(two instanceof IpAddress)) {
            if(log.isErrorEnabled()) log.error("addresses have to be of type IpAddress to be compared");
            return false;
        }

        a=((IpAddress)one).getIpAddress();
        b=((IpAddress)two).getIpAddress();
        if(a == null || b == null) return false;
        host_a=a.getHostAddress();
        host_b=b.getHostAddress();

        // System.out.println("host_a=" + host_a + ", host_b=" + host_b);
        return host_a.equals(host_b);
    }


    public static void removeFile(String fname) {
        if(fname == null) return;
        try {
            new File(fname).delete();
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error("exception=" + ex);
        }
    }


    public static boolean fileExists(String fname) {
        return (new File(fname)).exists();
    }


    /**
     E.g. 2000,4000,8000
     */
    public static long[] parseCommaDelimitedLongs(String s) {
        StringTokenizer tok;
        Vector v=new Vector();
        Long l;
        long[] retval=null;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            l=new Long(tok.nextToken());
            v.addElement(l);
        }
        if(v.size() == 0) return null;
        retval=new long[v.size()];
        for(int i=0; i < v.size(); i++)
            retval[i]=((Long)v.elementAt(i)).longValue();
        return retval;
    }

    /** e.g. "bela,jeannette,michelle" --> List{"bela", "jeannette", "michelle"} */
    public static java.util.List parseCommaDelimitedStrings(String l) {
        java.util.List tmp=new ArrayList();
        StringTokenizer tok=new StringTokenizer(l, ",");
        String t;

        while(tok.hasMoreTokens()) {
            t=tok.nextToken();
            tmp.add(t);
        }

        return tmp;
    }


    public static String shortName(String hostname) {
        int index;
        StringBuffer sb=new StringBuffer();

        if(hostname == null) return null;

        index=hostname.indexOf('.');
        if(index > 0 && !Character.isDigit(hostname.charAt(0)))
            sb.append(hostname.substring(0, index));
        else
            sb.append(hostname);
        return sb.toString();
    }


    /** Finds first available port starting at start_port and returns server socket */
    public static ServerSocket createServerSocket(int start_port) {
        ServerSocket ret=null;

        while(true) {
            try {
                ret=new ServerSocket(start_port);
            }
            catch(BindException bind_ex) {
                start_port++;
                continue;
            }
            catch(IOException io_ex) {
                if(log.isErrorEnabled()) log.error("exception is " + io_ex);
            }
            break;
        }
        return ret;
    }

    public static ServerSocket createServerSocket(InetAddress bind_addr, int start_port) {
        ServerSocket ret=null;

        while(true) {
            try {
                ret=new ServerSocket(start_port, 50, bind_addr);
            }
            catch(BindException bind_ex) {
                start_port++;
                continue;
            }
            catch(IOException io_ex) {
                if(log.isErrorEnabled()) log.error("exception is " + io_ex);
            }
            break;
        }
        return ret;
    }



    /**
     * Creates a DatagramSocket bound to addr. If addr is null, socket won't be bound. If address is already in use,
     * start_port will be incremented until a socket can be created.
     * @param addr The InetAddress to which the socket should be bound. If null, the socket will not be bound.
     * @param port The port which the socket should use. If 0, a random port will be used. If > 0, but port is already
     *             in use, it will be incremented until an unused port is found, or until MAX_PORT is reached.
     */
    public static DatagramSocket createDatagramSocket(InetAddress addr, int port) throws Exception {
        DatagramSocket sock=null;

        if(addr == null) {
            if(port == 0) {
                return new DatagramSocket();
            }
            else {
                while(port < MAX_PORT) {
                    try {
                        return new DatagramSocket(port);
                    }
                    catch(BindException bind_ex) { // port already used
                        port++;
                        continue;
                    }
                    catch(Exception ex) {
                        throw ex;
                    }
                }
            }
        }
        else {
            if(port == 0) port=1024;
            while(port < MAX_PORT) {
                try {
                    return new DatagramSocket(port, addr);
                }
                catch(BindException bind_ex) { // port already used
                    port++;
                    continue;
                }
                catch(Exception ex) {
                    throw ex;
                }
            }
        }
        return sock; // will never be reached, but the stupid compiler didn't figure it out...
    }


    public static boolean checkForLinux() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("linux") ? true : false;
    }

    public static boolean checkForSolaris() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("sun") ? true : false;
    }

    public static boolean checkForWindows() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("win") ? true : false;
    }

    public static void prompt(String s) {
        System.out.println(s);
        System.out.flush();
        try {
            while(System.in.available() > 0)
                System.in.read();
            System.in.read();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    public static String memStats(boolean gc) {
        StringBuffer sb=new StringBuffer();
        Runtime rt=Runtime.getRuntime();
        if(gc)
            rt.gc();
        long free_mem, total_mem, used_mem;
        free_mem=rt.freeMemory();
        total_mem=rt.totalMemory();
        used_mem=total_mem - free_mem;
        sb.append("Free mem: ").append(free_mem).append("\nUsed mem: ").append(used_mem);
        sb.append("\nTotal mem: ").append(total_mem);
        return sb.toString();
    }


    /*
    public static void main(String[] args) {
	DatagramSocket sock;
	InetAddress    addr=null;
	int            port=0;

	for(int i=0; i < args.length; i++) {
	    if(args[i].equals("-help")) {
		System.out.println("Util [-help] [-addr] [-port]");
		return;
	    }
	    if(args[i].equals("-addr")) {
		try {
		    addr=InetAddress.getByName(args[++i]);
		    continue;
		}
		catch(Exception ex) {
		    System.err.println(ex);
		    return;
		}
	    }
	    if(args[i].equals("-port")) {
		port=Integer.parseInt(args[++i]);
		continue;
	    }
	    System.out.println("Util [-help] [-addr] [-port]");
	    return;
	}

	try {
	    sock=createDatagramSocket(addr, port);
	    System.out.println("sock: local address is " + sock.getLocalAddress() + ":" + sock.getLocalPort() +
			       ", remote address is " + sock.getInetAddress() + ":" + sock.getPort());
	    System.in.read();
	}
	catch(Exception ex) {
	    System.err.println(ex);
	}
    }
    */

    public static void main(String args[]) {
        System.out.println("Check for Linux:   " + checkForLinux());
        System.out.println("Check for Solaris: " + checkForSolaris());
        System.out.println("Check for Windows: " + checkForWindows());
    }



}
