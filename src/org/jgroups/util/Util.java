package org.jgroups.util;

import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.auth.AuthToken;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.UdpHeader;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.stack.IpAddress;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.text.NumberFormat;
import java.util.*;
import java.util.List;


/**
 * Collection of various utility routines that can not be assigned to other classes.
 * @author Bela Ban
 * @version $Id: Util.java,v 1.112 2006/12/31 14:31:00 belaban Exp $
 */
public class Util {
    private static final ByteArrayOutputStream out_stream=new ByteArrayOutputStream(512);

    private static  NumberFormat f;

    private static Map PRIMITIVE_TYPES=new HashMap(10);
    private static final byte TYPE_NULL         =  0;
    private static final byte TYPE_STREAMABLE   =  1;
    private static final byte TYPE_SERIALIZABLE =  2;

    private static final byte TYPE_BOOLEAN      = 10;
    private static final byte TYPE_BYTE         = 11;
    private static final byte TYPE_CHAR         = 12;
    private static final byte TYPE_DOUBLE       = 13;
    private static final byte TYPE_FLOAT        = 14;
    private static final byte TYPE_INT          = 15;
    private static final byte TYPE_LONG         = 16;
    private static final byte TYPE_SHORT        = 17;
    private static final byte TYPE_STRING       = 18;

    // constants
    public static final int MAX_PORT=65535; // highest port allocatable
    static boolean resolve_dns=false;

    static boolean      JGROUPS_COMPAT=false;

    /**
     * Global thread group to which all (most!) JGroups threads belong
     */
    private static ThreadGroup GLOBAL_GROUP=new ThreadGroup("JGroups") {
        public void uncaughtException(Thread t, Throwable e) {
            LogFactory.getLog("org.jgroups").error("uncaught exception in " + t + " (thread group=" + GLOBAL_GROUP + " )", e);
        }
    };

    public static ThreadGroup getGlobalThreadGroup() {
        return GLOBAL_GROUP;
    }


    static {
        /* Trying to get value of resolve_dns. PropertyPermission not granted if
        * running in an untrusted environment  with JNLP */
        try {
            resolve_dns=Boolean.valueOf(System.getProperty("resolve.dns", "false")).booleanValue();
        }
        catch (SecurityException ex){
            resolve_dns=false;
        }
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);

        try {
            String tmp=Util.getProperty(new String[]{Global.MARSHALLING_COMPAT}, null, null, false, "false");
            JGROUPS_COMPAT=Boolean.valueOf(tmp).booleanValue();
        }
        catch (SecurityException ex){
        }

        PRIMITIVE_TYPES.put(Boolean.class, new Byte(TYPE_BOOLEAN));
        PRIMITIVE_TYPES.put(Byte.class, new Byte(TYPE_BYTE));
        PRIMITIVE_TYPES.put(Character.class, new Byte(TYPE_CHAR));
        PRIMITIVE_TYPES.put(Double.class, new Byte(TYPE_DOUBLE));
        PRIMITIVE_TYPES.put(Float.class, new Byte(TYPE_FLOAT));
        PRIMITIVE_TYPES.put(Integer.class, new Byte(TYPE_INT));
        PRIMITIVE_TYPES.put(Long.class, new Byte(TYPE_LONG));
        PRIMITIVE_TYPES.put(Short.class, new Byte(TYPE_SHORT));
        PRIMITIVE_TYPES.put(String.class, new Byte(TYPE_STRING));
    }


    /**
     * Verifies that val is <= max memory
     * @param buf_name
     * @param val
     */
    public static void checkBufferSize(String buf_name, long val) {
        // sanity check that max_credits doesn't exceed memory allocated to VM by -Xmx
        long max_mem=Runtime.getRuntime().maxMemory();
        if(val > max_mem) {
            throw new IllegalArgumentException(buf_name + "(" + Util.printBytes(val) + ") exceeds max memory allocated to VM (" +
                    Util.printBytes(max_mem) + ")");
        }
    }


    public static void close(InputStream inp) {
        if(inp != null)
            try {inp.close();} catch(IOException e) {}
    }

    public static void close(OutputStream out) {
        if(out != null) {
            try {out.close();} catch(IOException e) {}
        }
    }

    public static void close(Socket s) {
        if(s != null) {
            try {s.close();} catch(Exception ex) {}
        }
    }

     public static void close(DatagramSocket my_sock) {
        if(my_sock != null) {
            try {my_sock.close();} catch(Throwable t) {}
        }
    }




    /**
     * Creates an object from a byte buffer
     */
    public static Object objectFromByteBuffer(byte[] buffer) throws Exception {
        if(buffer == null) return null;
        if(JGROUPS_COMPAT)
            return oldObjectFromByteBuffer(buffer);
        return objectFromByteBuffer(buffer, 0, buffer.length);
    }


    public static Object objectFromByteBuffer(byte[] buffer, int offset, int length) throws Exception {
        if(buffer == null) return null;
        if(JGROUPS_COMPAT)
            return oldObjectFromByteBuffer(buffer, offset, length);
        Object retval=null;
        InputStream in=null;
        ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer, offset, length);
        byte b=(byte)in_stream.read();

        try {
            switch(b) {
                case TYPE_NULL:
                    return null;
                case TYPE_STREAMABLE:
                    in=new DataInputStream(in_stream);
                    retval=readGenericStreamable((DataInputStream)in);
                    break;
                case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                    in=new ContextObjectInputStream(in_stream); // changed Nov 29 2004 (bela)
                    retval=((ContextObjectInputStream)in).readObject();
                    break;
                case TYPE_BOOLEAN:
                    in=new DataInputStream(in_stream);
                    retval=Boolean.valueOf(((DataInputStream)in).readBoolean());
                    break;
                case TYPE_BYTE:
                    in=new DataInputStream(in_stream);
                    retval=new Byte(((DataInputStream)in).readByte());
                    break;
                case TYPE_CHAR:
                    in=new DataInputStream(in_stream);
                    retval=new Character(((DataInputStream)in).readChar());
                    break;
                case TYPE_DOUBLE:
                    in=new DataInputStream(in_stream);
                    retval=new Double(((DataInputStream)in).readDouble());
                    break;
                case TYPE_FLOAT:
                    in=new DataInputStream(in_stream);
                    retval=new Float(((DataInputStream)in).readFloat());
                    break;
                case TYPE_INT:
                    in=new DataInputStream(in_stream);
                    retval=new Integer(((DataInputStream)in).readInt());
                    break;
                case TYPE_LONG:
                    in=new DataInputStream(in_stream);
                    retval=new Long(((DataInputStream)in).readLong());
                    break;
                case TYPE_SHORT:
                    in=new DataInputStream(in_stream);
                    retval=new Short(((DataInputStream)in).readShort());
                    break;
                case TYPE_STRING:
                    in=new DataInputStream(in_stream);
                    retval=((DataInputStream)in).readUTF();
                    break;
                default:
                    throw new IllegalArgumentException("type " + b + " is invalid");
            }
            return retval;
        }
        finally {
            Util.close(in);
        }
    }




    /**
     * Serializes/Streams an object into a byte buffer.
     * The object has to implement interface Serializable or Externalizable
     * or Streamable.  Only Streamable objects are interoperable w/ jgroups-me
     */
    public static byte[] objectToByteBuffer(Object obj) throws Exception {

        if(JGROUPS_COMPAT)
            return oldObjectToByteBuffer(obj);

        byte[] result=null;

        synchronized(out_stream) {
            out_stream.reset();
            if(obj == null) {
                out_stream.write(TYPE_NULL);
                out_stream.flush();
                return out_stream.toByteArray();
            }

            OutputStream out=null;
            Byte type;
            try {
                if(obj instanceof Streamable) {  // use Streamable if we can
                    out_stream.write(TYPE_STREAMABLE);
                    out=new DataOutputStream(out_stream);
                    writeGenericStreamable((Streamable)obj, (DataOutputStream)out);
                }
                else if((type=(Byte)PRIMITIVE_TYPES.get(obj.getClass())) != null) {
                    out_stream.write(type.byteValue());
                    out=new DataOutputStream(out_stream);
                    switch(type.byteValue()) {
                        case TYPE_BOOLEAN:
                            ((DataOutputStream)out).writeBoolean(((Boolean)obj).booleanValue());
                            break;
                        case TYPE_BYTE:
                            ((DataOutputStream)out).writeByte(((Byte)obj).byteValue());
                            break;
                        case TYPE_CHAR:
                            ((DataOutputStream)out).writeChar(((Character)obj).charValue());
                            break;
                        case TYPE_DOUBLE:
                            ((DataOutputStream)out).writeDouble(((Double)obj).doubleValue());
                            break;
                        case TYPE_FLOAT:
                            ((DataOutputStream)out).writeFloat(((Float)obj).floatValue());
                            break;
                        case TYPE_INT:
                            ((DataOutputStream)out).writeInt(((Integer)obj).intValue());
                            break;
                        case TYPE_LONG:
                            ((DataOutputStream)out).writeLong(((Long)obj).longValue());
                            break;
                        case TYPE_SHORT:
                            ((DataOutputStream)out).writeShort(((Short)obj).shortValue());
                            break;
                        case TYPE_STRING:
                            ((DataOutputStream)out).writeUTF((String)obj);
                            break;
                        default:
                            throw new IllegalArgumentException("type " + type + " is invalid");
                    }
                }
                else { // will throw an exception if object is not serializable
                    out_stream.write(TYPE_SERIALIZABLE);
                    out=new ObjectOutputStream(out_stream);
                    ((ObjectOutputStream)out).writeObject(obj);
                }
            }
            finally {
                Util.close(out);
            }
            result=out_stream.toByteArray();
        }
        return result;
    }




    /** For backward compatibility in JBoss 4.0.2 */
    public static Object oldObjectFromByteBuffer(byte[] buffer) throws Exception {
        if(buffer == null) return null;
        return oldObjectFromByteBuffer(buffer, 0, buffer.length);
    }

    public static Object oldObjectFromByteBuffer(byte[] buffer, int offset, int length) throws Exception {
        if(buffer == null) return null;
        Object retval=null;

        try {  // to read the object as an Externalizable
            ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer, offset, length);
            ObjectInputStream in=new ContextObjectInputStream(in_stream); // changed Nov 29 2004 (bela)
            retval=in.readObject();
            in.close();
        }
        catch(StreamCorruptedException sce) {
            try {  // is it Streamable?
                ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer, offset, length);
                DataInputStream in=new DataInputStream(in_stream);
                retval=readGenericStreamable(in);
                in.close();
            }
            catch(Exception ee) {
                IOException tmp=new IOException("unmarshalling failed");
                tmp.initCause(ee);
                throw tmp;
            }
        }

        if(retval == null)
            return null;
        return retval;
    }




    /**
     * Serializes/Streams an object into a byte buffer.
     * The object has to implement interface Serializable or Externalizable
     * or Streamable.  Only Streamable objects are interoperable w/ jgroups-me
     */
    public static byte[] oldObjectToByteBuffer(Object obj) throws Exception {
        byte[] result=null;
        synchronized(out_stream) {
            out_stream.reset();
            if(obj instanceof Streamable) {  // use Streamable if we can
                DataOutputStream out=new DataOutputStream(out_stream);
                writeGenericStreamable((Streamable)obj, out);
                out.close();
            }
            else {
                ObjectOutputStream out=new ObjectOutputStream(out_stream);
                out.writeObject(obj);
                out.close();
            }
            result=out_stream.toByteArray();
        }
        return result;
    }



    public static Streamable streamableFromByteBuffer(Class cl, byte[] buffer) throws Exception {
        if(buffer == null) return null;
        Streamable retval=null;
        ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer);
        DataInputStream in=new DataInputStream(in_stream); // changed Nov 29 2004 (bela)
        retval=(Streamable)cl.newInstance();
        retval.readFrom(in);
        in.close();
        if(retval == null)
            return null;
        return retval;
    }


    public static Streamable streamableFromByteBuffer(Class cl, byte[] buffer, int offset, int length) throws Exception {
        if(buffer == null) return null;
        Streamable retval=null;
        ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer, offset, length);
        DataInputStream in=new DataInputStream(in_stream); // changed Nov 29 2004 (bela)
        retval=(Streamable)cl.newInstance();
        retval.readFrom(in);
        in.close();
        if(retval == null)
            return null;
        return retval;
    }

    public static byte[] streamableToByteBuffer(Streamable obj) throws Exception {
        byte[] result=null;
        synchronized(out_stream) {
            out_stream.reset();
            DataOutputStream out=new DataOutputStream(out_stream);
            obj.writeTo(out);
            result=out_stream.toByteArray();
            out.close();
        }
        return result;
    }


    public static byte[] collectionToByteBuffer(Collection c) throws Exception {
        byte[] result=null;
        synchronized(out_stream) {
            out_stream.reset();
            DataOutputStream out=new DataOutputStream(out_stream);
            Util.writeAddresses(c, out);
            result=out_stream.toByteArray();
            out.close();
        }
        return result;
    }

    public static int size(Address addr) {
        int retval=Global.BYTE_SIZE; // presence byte
        if(addr != null)
            retval+=addr.size() + Global.BYTE_SIZE; // plus type of address
        return retval;
    }

    public static void writeAuthToken(AuthToken token, DataOutputStream out) throws IOException{
        Util.writeString(token.getName(), out);
        token.writeTo(out);
    }

    public static AuthToken readAuthToken(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        try{
            String type = Util.readString(in);
            Object obj = Class.forName(type).newInstance();
            AuthToken token = (AuthToken) obj;
            token.readFrom(in);
            return token;
        }
        catch(ClassNotFoundException cnfe){
            return null;
        }
    }

    public static void writeAddress(Address addr, DataOutputStream out) throws IOException {
        if(addr == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        if(addr instanceof IpAddress) {
            // regular case, we don't need to include class information about the type of Address, e.g. JmsAddress
            out.writeBoolean(true);
            addr.writeTo(out);
        }
        else {
            out.writeBoolean(false);
            writeOtherAddress(addr, out);
        }
    }

    public static Address readAddress(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        Address addr=null;
        if(in.readBoolean() == false)
            return null;
        if(in.readBoolean()) {
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

    /**
     * Writes a Vector of Addresses. Can contain 65K addresses at most
     * @param v A Collection<Address>
     * @param out
     * @throws IOException
     */
    public static void writeAddresses(Collection v, DataOutputStream out) throws IOException {
        if(v == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(v.size());
        Address addr;
        for(Iterator it=v.iterator(); it.hasNext();) {
            addr=(Address)it.next();
            Util.writeAddress(addr, out);
        }
    }

    /**
     *
     * @param in
     * @param cl The type of Collection, e.g. Vector.class
     * @return Collection of Address objects
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static Collection readAddresses(DataInputStream in, Class cl) throws IOException, IllegalAccessException, InstantiationException {
        short length=in.readShort();
        if(length < 0) return null;
        Collection retval=(Collection)cl.newInstance();
        Address addr;
        for(int i=0; i < length; i++) {
            addr=Util.readAddress(in);
            retval.add(addr);
        }
        return retval;
    }


    /**
     * Returns the marshalled size of a Collection of Addresses.
     * <em>Assumes elements are of the same type !</em>
     * @param addrs Collection<Address>
     * @return long size
     */
    public static long size(Collection addrs) {
        int retval=Global.SHORT_SIZE; // number of elements
        if(addrs != null && addrs.size() > 0) {
            Address addr=(Address)addrs.iterator().next();
            retval+=size(addr) * addrs.size();
        }
        return retval;
    }




    public static void writeStreamable(Streamable obj, DataOutputStream out) throws IOException {
        if(obj == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        obj.writeTo(out);
    }


    public static Streamable readStreamable(Class clazz, DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        Streamable retval=null;
        if(in.readBoolean() == false)
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
                if (clazz==null) {
                   throw new ClassNotFoundException("Class for magic number "+magic_number+" cannot be found.");
                }
            }
            else {
                classname=in.readUTF();
                clazz=ClassConfigurator.getInstance(false).get(classname);
                if (clazz==null) {
                   throw new ClassNotFoundException(classname);
                }
            }

            retval=(Streamable)clazz.newInstance();
            retval.readFrom(in);
            return retval;
        }
        catch(Exception ex) {
            throw new IOException("failed reading object: " + ex.toString());
        }
    }

    public static void writeObject(Object obj, DataOutputStream out) throws Exception {
       if(obj == null || !(obj instanceof Streamable)) {
           byte[] buf=objectToByteBuffer(obj);
           out.writeShort(buf.length);
           out.write(buf, 0, buf.length);
       }
       else {
           out.writeShort(-1);
           writeGenericStreamable((Streamable)obj, out);
       }
    }

    public static Object readObject(DataInputStream in) throws Exception {
        short len=in.readShort();
        Object retval=null;
        if(len == -1) {
            retval=readGenericStreamable(in);
        }
        else {
            byte[] buf=new byte[len];
            in.readFully(buf, 0, len);
            retval=objectFromByteBuffer(buf);
        }
        return retval;
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


    /**
       * Marshalls a list of messages.
       * @param xmit_list LinkedList<Message>
       * @return Buffer
       * @throws IOException
       */
    public static Buffer msgListToByteBuffer(LinkedList xmit_list) throws IOException {
        ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new DataOutputStream(output);
        Message msg;
        Buffer retval=null;

        out.writeInt(xmit_list.size());
        for(Iterator it=xmit_list.iterator(); it.hasNext();) {
            msg=(Message)it.next();
            msg.writeTo(out);
        }
        out.flush();
        retval=new Buffer(output.getRawBuffer(), 0, output.size());
        out.close();
        output.close();
        return retval;
    }

    public static LinkedList byteBufferToMessageList(byte[] buffer, int offset, int length) throws Exception {
        LinkedList retval=null;
        ByteArrayInputStream input=new ByteArrayInputStream(buffer, offset, length);
        DataInputStream in=new DataInputStream(input);
        int size=in.readInt();

        if(size == 0)
            return null;

        Message msg;
        retval=new LinkedList();
        for(int i=0; i < size; i++) {
            msg=new Message(false); // don't create headers, readFrom() will do this
            msg.readFrom(in);
            retval.add(msg);
        }

        return retval;
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
        catch(Throwable e) {
        }
    }


    public static void sleep(long timeout, int nanos) {
        try {
            Thread.sleep(timeout, nanos);
        }
        catch(Throwable e) {
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
        return r < cutoff;
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
     * Debugging method used to dump the content of a protocol queue in a condensed form. Useful
     * to follow the evolution of the queue's content in time.
     */
    public static String dumpQueue(Queue q) {
        StringBuilder sb=new StringBuilder();
        LinkedList values=q.values();
        if(values.size() == 0) {
            sb.append("empty");
        }
        else {
            for(Iterator it=values.iterator(); it.hasNext();) {
                Object o=it.next();
                String s=null;
                if(o instanceof Event) {
                    Event event=(Event)o;
                    int type=event.getType();
                    s=Event.type2String(type);
                    if(type == Event.VIEW_CHANGE)
                        s+=" " + event.getArg();
                    if(type == Event.MSG)
                        s+=" " + event.getArg();

                    if(type == Event.MSG) {
                        s+="[";
                        Message m=(Message)event.getArg();
                        Map headers=new HashMap(m.getHeaders());
                        for(Iterator i=headers.keySet().iterator(); i.hasNext();) {
                            Object headerKey=i.next();
                            Object value=headers.get(headerKey);
                            String headerToString=null;
                            if(value instanceof FD.FdHeader) {
                                headerToString=value.toString();
                            }
                            else
                                if(value instanceof PingHeader) {
                                    headerToString=headerKey + "-";
                                    if(((PingHeader)value).type == PingHeader.GET_MBRS_REQ) {
                                        headerToString+="GMREQ";
                                    }
                                    else
                                        if(((PingHeader)value).type == PingHeader.GET_MBRS_RSP) {
                                            headerToString+="GMRSP";
                                        }
                                        else {
                                            headerToString+="UNKNOWN";
                                        }
                                }
                                else {
                                    headerToString=headerKey + "-" + (value == null ? "null" : value.toString());
                                }
                            s+=headerToString;

                            if(i.hasNext()) {
                                s+=",";
                            }
                        }
                        s+="]";
                    }
                }
                else {
                    s=o.toString();
                }
                sb.append(s).append("\n");
            }
        }
        return sb.toString();
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
        StringBuilder sb=new StringBuilder();
        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        sb.append("------- Threads -------\n");
        for(int i=0; i < threads.length; i++) {
            sb.append("#").append(i).append(": ").append(threads[i]).append('\n');
        }
        sb.append("------- Threads -------\n");
        return sb.toString();
    }

    public static String printBytes(long bytes) {
        double tmp;

        if(bytes < 1000)
            return bytes + "b";
        if(bytes < 1000000) {
            tmp=bytes / 1000.0;
            return f.format(tmp) + "KB";
        }
        if(bytes < 1000000000) {
            tmp=bytes / 1000000.0;
            return f.format(tmp) + "MB";
        }
        else {
            tmp=bytes / 1000000000.0;
            return f.format(tmp) + "GB";
        }
    }


    public static String printBytes(double bytes) {
        double tmp;

        if(bytes < 1000)
            return bytes + "b";
        if(bytes < 1000000) {
            tmp=bytes / 1000.0;
            return f.format(tmp) + "KB";
        }
        if(bytes < 1000000000) {
            tmp=bytes / 1000000.0;
            return f.format(tmp) + "MB";
        }
        else {
            tmp=bytes / 1000000000.0;
            return f.format(tmp) + "GB";
        }
    }


    /**
     Fragments a byte buffer into smaller fragments of (max.) frag_size.
     Example: a byte buffer of 1024 bytes and a frag_size of 248 gives 4 fragments
     of 248 bytes each and 1 fragment of 32 bytes.
     @return An array of byte buffers (<code>byte[]</code>).
     */
    public static byte[][] fragmentBuffer(byte[] buf, int frag_size, final int length) {
        byte[] retval[];
        int accumulated_size=0;
        byte[] fragment;
        int tmp_size=0;
        int num_frags;
        int index=0;

        num_frags=length % frag_size == 0 ? length / frag_size : length / frag_size + 1;
        retval=new byte[num_frags][];

        while(accumulated_size < length) {
            if(accumulated_size + frag_size <= length)
                tmp_size=frag_size;
            else
                tmp_size=length - accumulated_size;
            fragment=new byte[tmp_size];
            System.arraycopy(buf, accumulated_size, fragment, 0, tmp_size);
            retval[index++]=fragment;
            accumulated_size+=tmp_size;
        }
        return retval;
    }

    public static byte[][] fragmentBuffer(byte[] buf, int frag_size) {
        return fragmentBuffer(buf, frag_size, buf.length);
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
        StringBuilder ret=new StringBuilder("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }

        ret.append(']');
        return ret.toString();
    }

    public static String array2String(int[] array) {
        StringBuilder ret=new StringBuilder("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }

        ret.append(']');
        return ret.toString();
    }

    public static String array2String(boolean[] array) {
        StringBuilder ret=new StringBuilder("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }
        ret.append(']');
        return ret.toString();
    }

    public static String array2String(Object[] array) {
        StringBuilder ret=new StringBuilder("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }
        ret.append(']');
        return ret.toString();
    }

    /** Returns true if all elements of c match obj */
    public static boolean all(Collection c, Object obj) {
        for(Iterator iterator=c.iterator(); iterator.hasNext();) {
            Object o=iterator.next();
            if(!o.equals(obj))
                return false;
        }
        return true;
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


    public static Object pickRandomElement(Vector list) {
        if(list == null) return null;
        int size=list.size();
        int index=(int)((Math.random() * size * 10) % size);
        return list.get(index);
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
        StringBuilder sb=new StringBuilder("(");
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
     but before we send data). Two writes ensure that, if the peer closed the connection, the first write
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


    /**
     Makes sure that we detect when a peer connection is in the closed state (not closed while we send data,
     but before we send data). Two writes ensure that, if the peer closed the connection, the first write
     will send the peer from FIN to RST state, and the second will cause a signal (IOException).
     */
    public static void doubleWrite(byte[] buf, int offset, int length, OutputStream out) throws Exception {
        if(length > 1) {
            out.write(buf, offset, 1);
            out.write(buf, offset+1, length - 1);
        }
        else {
            out.write(buf, offset, 0);
            out.write(buf, offset, length);
        }
    }

    /**
    * if we were to register for OP_WRITE and send the remaining data on
    * readyOps for this channel we have to either block the caller thread or
    * queue the message buffers that may arrive while waiting for OP_WRITE.
    * Instead of the above approach this method will continuously write to the
    * channel until the buffer sent fully.
    */
    public static void writeFully(ByteBuffer buf, WritableByteChannel out) throws IOException {
        int written = 0;
        int toWrite = buf.limit();
        while (written < toWrite) {
            written += out.write(buf);
        }
    }

//    /* double writes are not required.*/
//	public static void doubleWriteBuffer(
//		ByteBuffer buf,
//		WritableByteChannel out)
//		throws Exception
//	{
//		if (buf.limit() > 1)
//		{
//			int actualLimit = buf.limit();
//			buf.limit(1);
//			writeFully(buf,out);
//			buf.limit(actualLimit);
//			writeFully(buf,out);
//		}
//		else
//		{
//			buf.limit(0);
//			writeFully(buf,out);
//			buf.limit(1);
//			writeFully(buf,out);
//		}
//	}


    public static long sizeOf(String classname) {
        Object inst;
        byte[] data;

        try {
            inst=Util.loadClass(classname, null).newInstance();
            data=Util.objectToByteBuffer(inst);
            return data.length;
        }
        catch(Exception ex) {
            return -1;
        }
    }


    public static long sizeOf(Object inst) {
        byte[] data;

        try {
            data=Util.objectToByteBuffer(inst);
            return data.length;
        }
        catch(Exception ex) {
            return -1;
        }
    }

    public static long sizeOf(Streamable inst) {
        byte[] data;
        ByteArrayOutputStream output;
        DataOutputStream out;

        try {
            output=new ByteArrayOutputStream();
            out=new DataOutputStream(output);
            inst.writeTo(out);
            out.flush();
            data=output.toByteArray();
            return data.length;
        }
        catch(Exception ex) {
            return -1;
        }
    }



    /**
     * Tries to load the class from the current thread's context class loader. If
     * not successful, tries to load the class from the current instance.
     * @param classname Desired class.
     * @param clazz Class object used to obtain a class loader
     * 				if no context class loader is available.
     * @return Class, or null on failure.
     */
    public static Class loadClass(String classname, Class clazz) throws ClassNotFoundException {
        ClassLoader loader;

        try {
            loader=Thread.currentThread().getContextClassLoader();
            if(loader != null) {
                return loader.loadClass(classname);
            }
        }
        catch(Throwable t) {
        }

        if(clazz != null) {
            try {
                loader=clazz.getClassLoader();
                if(loader != null) {
                    return loader.loadClass(classname);
                }
            }
            catch(Throwable t) {
            }
        }

        try {
            loader=ClassLoader.getSystemClassLoader();
            if(loader != null) {
                return loader.loadClass(classname);
            }
        }
        catch(Throwable t) {
        }

        throw new ClassNotFoundException(classname);
    }


    public static InputStream getResourceAsStream(String name, Class clazz) {
        ClassLoader loader;

        try {
            loader=Thread.currentThread().getContextClassLoader();
            if(loader != null) {
                return loader.getResourceAsStream(name);
            }
        }
        catch(Throwable t) {
        }

        if(clazz != null) {
            try {
                loader=clazz.getClassLoader();
                if(loader != null) {
                    return loader.getResourceAsStream(name);
                }
            }
            catch(Throwable t) {
            }
        }
        try {
            loader=ClassLoader.getSystemClassLoader();
            if(loader != null) {
                return loader.getResourceAsStream(name);
            }
        }
        catch(Throwable t) {
        }

        return null;
    }


    /** Checks whether 2 Addresses are on the same host */
    public static boolean sameHost(Address one, Address two) {
        InetAddress a, b;
        String host_a, host_b;

        if(one == null || two == null) return false;
        if(!(one instanceof IpAddress) || !(two instanceof IpAddress)) {
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



    public static boolean fileExists(String fname) {
        return (new File(fname)).exists();
    }


    /**
     * Parses comma-delimited longs; e.g., 2000,4000,8000.
     * Returns array of long, or null.
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
        return parseStringList(l, ",");
    }


    public static List parseStringList(String l, String separator) {
         List tmp=new LinkedList();
         StringTokenizer tok=new StringTokenizer(l, separator);
         String t;

         while(tok.hasMoreTokens()) {
             t=tok.nextToken();
             tmp.add(t.trim());
         }

         return tmp;
     }

    public static int parseInt(Properties props,String property,int defaultValue)
    {
        int result = defaultValue;
        String str=props.getProperty(property);
        if(str != null) {
            result=Integer.parseInt(str);
            props.remove(property);
        }
        return result;
    }


    public static long parseLong(Properties props,String property,long defaultValue)
    {
        long result = defaultValue;
        String str=props.getProperty(property);
        if(str != null) {
            result=Integer.parseInt(str);
            props.remove(property);
        }
        return result;
    }

    public static boolean parseBoolean(Properties props,String property,boolean defaultValue)
    {
        boolean result = defaultValue;
        String str=props.getProperty(property);
        if(str != null) {
            result=str.equalsIgnoreCase("true");
            props.remove(property);
        }
        return result;
    }

    public static InetAddress parseBindAddress(Properties props, String property) throws UnknownHostException {
        InetAddress bind_addr=null;
        boolean ignore_systemprops=Util.isBindAddressPropertyIgnored();
        String str=Util.getProperty(new String[]{Global.BIND_ADDR, Global.BIND_ADDR_OLD}, props, "bind_addr",
                                    ignore_systemprops, null);
        if(str != null) {
            bind_addr=InetAddress.getByName(str);
            props.remove(property);
        }
        return bind_addr;
    }


    public static String shortName(String hostname) {
        int index;
        StringBuilder sb=new StringBuilder();

        if(hostname == null) return null;

        index=hostname.indexOf('.');
        if(index > 0 && !Character.isDigit(hostname.charAt(0)))
            sb.append(hostname.substring(0, index));
        else
            sb.append(hostname);
        return sb.toString();
    }

    public static String shortName(InetAddress hostname) {
        if(hostname == null) return null;
        StringBuilder sb=new StringBuilder();
        if(resolve_dns)
            sb.append(hostname.getHostName());
        else
            sb.append(hostname.getHostAddress());
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
        return os != null && os.toLowerCase().startsWith("linux");
    }

    public static boolean checkForSolaris() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("sun");
    }

    public static boolean checkForWindows() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("win");
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


    public static int getJavaVersion() {
        String version=System.getProperty("java.version");
        int retval=0;
        if(version != null) {
            if(version.startsWith("1.2"))
                return 12;
            if(version.startsWith("1.3"))
                return 13;
            if(version.startsWith("1.4"))
                return 14;
            if(version.startsWith("1.5"))
                return 15;
            if(version.startsWith("5"))
                return 15;
            if(version.startsWith("1.6"))
                return 16;
            if(version.startsWith("6"))
                return 16;
        }
        return retval;
    }

    public static Vector unmodifiableVector(Vector v) {
        if(v == null) return null;
        return new UnmodifiableVector(v);
    }

    public static String memStats(boolean gc) {
        StringBuilder sb=new StringBuilder();
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


//    public static InetAddress getFirstNonLoopbackAddress() throws SocketException {
//        Enumeration en=NetworkInterface.getNetworkInterfaces();
//        while(en.hasMoreElements()) {
//            NetworkInterface i=(NetworkInterface)en.nextElement();
//            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
//                InetAddress addr=(InetAddress)en2.nextElement();
//                if(!addr.isLoopbackAddress())
//                    return addr;
//            }
//        }
//        return null;
//    }


    public static InetAddress getFirstNonLoopbackAddress() throws SocketException {
        Enumeration en=NetworkInterface.getNetworkInterfaces();
        boolean preferIpv4=Boolean.getBoolean("java.net.preferIPv4Stack");
        boolean preferIPv6=Boolean.getBoolean("java.net.preferIPv6Addresses");
        while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                if(!addr.isLoopbackAddress()) {
                    if(addr instanceof Inet4Address) {
                        if(preferIPv6)
                            continue;
                        return addr;
                    }
                    if(addr instanceof Inet6Address) {
                        if(preferIpv4)
                            continue;
                        return addr;
                    }
                }
            }
        }
        return null;
    }



    public static InetAddress getFirstNonLoopbackIPv6Address() throws SocketException {
        Enumeration en=NetworkInterface.getNetworkInterfaces();
        boolean preferIpv4=false;
        boolean preferIPv6=true;
        while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                if(!addr.isLoopbackAddress()) {
                    if(addr instanceof Inet4Address) {
                        if(preferIPv6)
                            continue;
                        return addr;
                    }
                    if(addr instanceof Inet6Address) {
                        if(preferIpv4)
                            continue;
                        return addr;
                    }
                }
            }
        }
        return null;
    }

    public static List getAllAvailableInterfaces() throws SocketException {
        List retval=new ArrayList(10);
        NetworkInterface intf;
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            intf=(NetworkInterface)en.nextElement();
            retval.add(intf);
        }
        return retval;
    }


    /**
     * Returns a value associated wither with one or more system properties, or found in the props map
     * @param system_props
     * @param props List of properties read from the configuration file
     * @param prop_name The name of the property, will be removed from props if found
     * @param ignore_sysprops If true, system properties are not used and the values will only be retrieved from
     * props (not system_props)
     * @param default_value Used to return a default value if the properties or system properties didn't have the value
     * @return The value, or null if not found
     */
    public static String getProperty(String[] system_props, Properties props, String prop_name,
                                     boolean ignore_sysprops, String default_value) {
        String retval=null;
        if(props != null && prop_name != null) {
            retval=props.getProperty(prop_name);
            props.remove(prop_name);
        }

        if(!ignore_sysprops) {
            String tmp, prop;
            if(system_props != null) {
                for(int i=0; i < system_props.length; i++) {
                    prop=system_props[i];
                    if(prop != null) {
                        try {
                            tmp=System.getProperty(prop);
                            if(tmp != null)
                                return tmp; // system properties override config file definitions
                        }
                        catch(SecurityException ex) {}
                    }
                }
            }
        }
        if(retval == null)
            return default_value;
        return retval;
    }



    public static boolean isBindAddressPropertyIgnored() {
        try {
            String tmp=System.getProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY);
            if(tmp == null) {
                tmp=System.getProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD);
                if(tmp == null)
                    return false;
            }
            tmp=tmp.trim().toLowerCase();
            return !(tmp.equals("false") || tmp.equals("no") || tmp.equals("off")) && (tmp.equals("true") || tmp.equals("yes") || tmp.equals("on"));
        }
        catch(SecurityException ex) {
            return false;
        }
    }


    public static MBeanServer getMBeanServer() {
        ArrayList servers=MBeanServerFactory.findMBeanServer(null);
        if(servers == null || servers.size() == 0)
            return null;

        // return 'jboss' server if available
        for(int i=0; i < servers.size(); i++) {
            MBeanServer srv=(MBeanServer)servers.get(i);
            if("jboss".equalsIgnoreCase(srv.getDefaultDomain()))
                return srv;
        }

        // return first available server
        return (MBeanServer)servers.get(0);
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
		    log.error(ex);
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
	    log.error(ex);
	}
    }
    */

    public static void main(String args[]) throws Exception {
        ClassConfigurator.getInstance(true);

        Message msg=new Message(null, new IpAddress("127.0.0.1", 4444), "Bela");
        long    size=Util.sizeOf(msg);
        System.out.println("size=" + msg.size() + ", streamable size=" + size);

        msg.putHeader("belaban", new NakAckHeader((byte)1, 23, 34));
        size=Util.sizeOf(msg);
        System.out.println("size=" + msg.size() + ", streamable size=" + size);

        msg.putHeader("bla", new UdpHeader("groupname"));
        size=Util.sizeOf(msg);
        System.out.println("size=" + msg.size() + ", streamable size=" + size);


        IpAddress a1=new IpAddress(1234), a2=new IpAddress("127.0.0.1", 3333);
        a1.setAdditionalData("Bela".getBytes());
        size=Util.sizeOf(a1);
        System.out.println("size=" + a1.size() + ", streamable size of a1=" + size);
        size=Util.sizeOf(a2);
        System.out.println("size=" + a2.size() + ", streamable size of a2=" + size);


//        System.out.println("Check for Linux:   " + checkForLinux());
//        System.out.println("Check for Solaris: " + checkForSolaris());
//        System.out.println("Check for Windows: " + checkForWindows());
//        System.out.println("version: " + getJavaVersion());
    }


    public static String generateList(Collection c, String separator) {
        if(c == null) return null;
        StringBuilder sb=new StringBuilder();
        boolean first=true;

        for(Iterator it=c.iterator(); it.hasNext();) {
            if(first) {
                first=false;
            }
            else {
                sb.append(separator);
            }
            sb.append(it.next());
        }
        return sb.toString();
    }

    /**
     * Replaces variables of ${var:default} with System.getProperty(var, default). If no variables are found, returns
     * the same string, otherwise a copy of the string with variables substituted
     * @param val
     * @return A string with vars replaced, or the same string if no vars found
     */
    public static String substituteVariable(String val) {
        if(val == null)
            return val;
        String retval=val, prev;

        while(retval.indexOf("${") >= 0) { // handle multiple variables in val
            prev=retval;
            retval=_substituteVar(retval);
            if(retval.equals(prev))
                break;
        }
        return retval;
    }

    private static String _substituteVar(String val) {
        int start_index, end_index;
        start_index=val.indexOf("${");
        if(start_index == -1)
            return val;
        end_index=val.indexOf("}", start_index+2);
        if(end_index == -1)
            throw new IllegalArgumentException("missing \"}\" in " + val);

        String tmp=getProperty(val.substring(start_index +2, end_index));
        if(tmp == null)
            return val;
        StringBuilder sb=new StringBuilder();
        sb.append(val.substring(0, start_index));
        sb.append(tmp);
        sb.append(val.substring(end_index+1));
        return sb.toString();
    }

    private static String getProperty(String s) {
        String var, default_val, retval=null;
        int index=s.indexOf(":");
        if(index >= 0) {
            var=s.substring(0, index);
            default_val=s.substring(index+1);
            retval=System.getProperty(var, default_val);
        }
        else {
            var=s;
            retval=System.getProperty(var);
        }
        return retval;
    }



}






