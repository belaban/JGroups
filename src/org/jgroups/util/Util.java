// $Id: Util.java,v 1.2 2003/12/24 01:45:19 belaban Exp $

package org.jgroups.util;

import java.io.*;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.ArrayList;

import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;


/**
 * Collection of various utility routines that can not be assigned to other classes.
 */
public class Util {
    private static Object mutex=new Object();
    private static ByteArrayOutputStream out_stream=new ByteArrayOutputStream(65535);

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
            ObjectInputStream in=new ObjectInputStream(in_stream);
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
            Trace.println("Util.sleepRandom()", Trace.ERROR, "timeout must be > 0 !");
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
                if(msg.getBuffer() != null)
                    return printMessage(msg);
                else
                    return msg.printObjectHeaders();
            }
        }
        return evt.toString();
    }


    /** Tries to read an object from the message's buffer and prints it */
    public static String printMessage(Message msg) {
        Object obj;

        if(msg == null)
            return "";
        byte[] buf=msg.getBuffer();
        if(buf == null)
            return null;

        try {
            obj=objectFromByteBuffer(buf);
            return obj.toString();
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
        byte[] buf=msg.getBuffer();
        if(buf == null)
            return "";

        try {
            obj=objectFromByteBuffer(buf);
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
            sb.append("#" + i + ": " + threads[i] + "\n");
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
            System.out.println("'" + new String(((byte[])frags[i])) + "'");
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

        ret.append("]");
        return ret.toString();
    }

    public static String array2String(int[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i] + " ");
        }

        ret.append("]");
        return ret.toString();
    }

    public static String array2String(boolean[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i] + " ");
        }
        ret.append("]");
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
        sb.append(")");
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
            Trace.error("Util.sizeOf()", "exception=" + ex);
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
            Trace.error("Util.sizeOf()", "exception+" + ex);
            return 0;
        }
    }


    /** Checks whether 2 Addresses are on the same host */
    public static boolean sameHost(Address one, Address two) {
        InetAddress a, b;
        String host_a, host_b;

        if(one == null || two == null) return false;
        if(!(one instanceof IpAddress) || !(two instanceof IpAddress)) {
            Trace.error("Util.sameHost()", "addresses have to be of type IpAddress to be compared");
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
            Trace.error("Util.removeFile()", "exception=" + ex);
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
        String host;
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
                Trace.error("Util.createServerSocket()", "exception is " + io_ex);
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
