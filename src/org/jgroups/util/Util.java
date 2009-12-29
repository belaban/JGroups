package org.jgroups.util;

import org.jgroups.*;
import org.jgroups.auth.AuthToken;
import org.jgroups.blocks.Connection;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.ProtocolStack;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


/**
 * Collection of various utility routines that can not be assigned to other classes.
 * @author Bela Ban
 * @version $Id: Util.java,v 1.247 2009/12/29 16:11:15 belaban Exp $
 */
public class Util {

    private static  NumberFormat f;

    private static Map<Class,Byte> PRIMITIVE_TYPES=new HashMap<Class,Byte>(10);
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
    private static final byte TYPE_BYTEARRAY    = 19;

    // constants
    public static final int MAX_PORT=65535; // highest port allocatable
    static boolean resolve_dns=false;

    static boolean      JGROUPS_COMPAT=false;

    private static short COUNTER=1;

    private static Pattern METHOD_NAME_TO_ATTR_NAME_PATTERN=Pattern.compile("[A-Z]+");
    private static Pattern ATTR_NAME_TO_METHOD_NAME_PATTERN=Pattern.compile("_.");

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

    private static Method NETWORK_INTERFACE_IS_UP=null;
    private static Method NETWORK_INTERFACE_IS_LOOPBACK=null;


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
        PRIMITIVE_TYPES.put(byte[].class, new Byte(TYPE_BYTEARRAY));

        try {
            NETWORK_INTERFACE_IS_UP=NetworkInterface.class.getMethod("isUp");
        }
        catch(Throwable e) {
        }

        try {
            NETWORK_INTERFACE_IS_LOOPBACK=NetworkInterface.class.getMethod("isLoopback");
        }
        catch(Throwable e) {
        }
    }



    public static void assertTrue(boolean condition) {
        assert condition;
    }

    public static void assertTrue(String message, boolean condition) {
        if(message != null)
            assert condition : message;
        else
            assert condition;
    }

    public static void assertFalse(boolean condition) {
        assertFalse(null, condition);
    }

    public static void assertFalse(String message, boolean condition) {
        if(message != null)
            assert !condition : message;
        else
            assert !condition;
    }


    public static void assertEquals(String message, Object val1, Object val2) {
        if(message != null) {
            assert val1.equals(val2) : message;
        }
        else {
            assert val1.equals(val2);
        }
    }

    public static void assertEquals(Object val1, Object val2) {
        assertEquals(null, val1, val2);
    }

    public static void assertNotNull(String message, Object val) {
        if(message != null)
            assert val != null : message;
        else
            assert val != null;
    }


    public static void assertNotNull(Object val) {
        assertNotNull(null, val);
    }


    public static void assertNull(String message, Object val) {
        if(message != null)
            assert val == null : message;
        else
            assert val == null;
    }



    /**
     * Blocks until all channels have the same view
     * @param timeout How long to wait (max in ms)
     * @param interval Check every interval ms
     * @param channels The channels which should form the view. The expected view size is channels.length.
     * Must be non-null
     */
    public static void blockUntilViewsReceived(long timeout, long interval, Channel ... channels) throws TimeoutException {
        final int expected_size=channels.length;

        if(interval > timeout)
            throw new IllegalArgumentException("interval needs to be smaller than timeout");
        final long end_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < end_time) {
            boolean all_ok=true;
            for(Channel ch: channels) {
                View view=ch.getView();
                if(view == null || view.size() != expected_size) {
                    all_ok=false;
                    break;
                }
            }
            if(all_ok)
                return;
            Util.sleep(interval);
        }
        throw new TimeoutException();
    }


    public static void addFlush(Channel ch, FLUSH flush) {
        if(ch == null || flush == null)
            throw new IllegalArgumentException("ch and flush have to be non-null");
        ProtocolStack stack=ch.getProtocolStack();
        stack.insertProtocolAtTop(flush);
    }


   


    /**
     * Utility method. If the dest address is IPv6, convert scoped link-local addrs into unscoped ones
     * @param sock
     * @param dest
     * @param sock_conn_timeout
     * @throws IOException
     */
    public static void connect(Socket sock, SocketAddress dest, int sock_conn_timeout) throws IOException {
        if(dest instanceof InetSocketAddress) {
            InetAddress addr=((InetSocketAddress)dest).getAddress();
            if(addr instanceof Inet6Address) {
                Inet6Address tmp=(Inet6Address)addr;
                if(tmp.getScopeId() != 0) {
                    dest=new InetSocketAddress(InetAddress.getByAddress(tmp.getAddress()), ((InetSocketAddress)dest).getPort());
                }
            }
        }
        sock.connect(dest, sock_conn_timeout);
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

    public static void close(ServerSocket s) {
        if(s != null) {
            try {s.close();} catch(Exception ex) {}
        }
    }

    public static void close(DatagramSocket my_sock) {
        if(my_sock != null) {
            try {my_sock.close();} catch(Throwable t) {}
        }
    }

    public static void close(Channel ch) {
        if(ch != null) {
            try {ch.close();} catch(Throwable t) {}
        }
    }

    public static void close(Channel ... channels) {
        if(channels != null) {
            for(Channel ch: channels)
                Util.close(ch);
        }
    }

    public static void close(Connection conn) {
        if(conn != null) {
            try {conn.close();} catch(Throwable t) {}
        }
    }

    /** Drops messages to/from other members and then closes the channel. Note that this member won't get excluded from
     * the view until failure detection has kicked in and the new coord installed the new view */
    public static void shutdown(Channel ch) throws Exception {
        DISCARD discard=new DISCARD();
        discard.setLocalAddress(ch.getAddress());
        discard.setDiscardAll(true);
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        stack.insertProtocol(discard,  ProtocolStack.ABOVE, transport.getClass());
        
        //abruptly shutdown FD_SOCK just as in real life when member gets killed non gracefully
        FD_SOCK fd = (FD_SOCK) ch.getProtocolStack().findProtocol("FD_SOCK");
        if(fd != null)
            fd.stopServerSocket(false);
        
        View view=ch.getView();
        if (view != null) {
            ViewId vid = view.getViewId();
            List<Address> members = Arrays.asList(ch.getAddress());

            ViewId new_vid = new ViewId(ch.getAddress(), vid.getId() + 1);
            View new_view = new View(new_vid, members);

            // inject view in which the shut down member is the only element
            GMS gms = (GMS) stack.findProtocol(GMS.class);
            gms.installView(new_view);
        }
        Util.close(ch);
    }


    public static byte setFlag(byte bits, byte flag) {
        return bits |= flag;
    }

    public static boolean isFlagSet(byte bits, byte flag) {
        return (bits & flag) == flag;
    }

    public static byte clearFlags(byte bits, byte flag) {
        return bits &= ~flag;
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
                    in=new ObjectInputStream(in_stream); // changed Nov 29 2004 (bela)
                    retval=((ObjectInputStream)in).readObject();
                    break;
                case TYPE_BOOLEAN:
                    in=new DataInputStream(in_stream);
                    retval=Boolean.valueOf(((DataInputStream)in).readBoolean());
                    break;
                case TYPE_BYTE:
                    in=new DataInputStream(in_stream);
                    retval=Byte.valueOf(((DataInputStream)in).readByte());
                    break;
                case TYPE_CHAR:
                    in=new DataInputStream(in_stream);
                    retval=Character.valueOf(((DataInputStream)in).readChar());
                    break;
                case TYPE_DOUBLE:
                    in=new DataInputStream(in_stream);
                    retval=Double.valueOf(((DataInputStream)in).readDouble());
                    break;
                case TYPE_FLOAT:
                    in=new DataInputStream(in_stream);
                    retval=Float.valueOf(((DataInputStream)in).readFloat());
                    break;
                case TYPE_INT:
                    in=new DataInputStream(in_stream);
                    retval=Integer.valueOf(((DataInputStream)in).readInt());
                    break;
                case TYPE_LONG:
                    in=new DataInputStream(in_stream);
                    retval=Long.valueOf(((DataInputStream)in).readLong());
                    break;
                case TYPE_SHORT:
                    in=new DataInputStream(in_stream);
                    retval=Short.valueOf(((DataInputStream)in).readShort());
                    break;
                case TYPE_STRING:
                    in=new DataInputStream(in_stream);
                    if(((DataInputStream)in).readBoolean()) { // large string
                        ObjectInputStream ois=new ObjectInputStream(in);
                        try {
                            return ois.readObject();
                        }
                        finally {
                            ois.close();
                        }
                    }
                    else {
                        retval=((DataInputStream)in).readUTF();
                    }
                    break;
                case TYPE_BYTEARRAY:
                    in=new DataInputStream(in_stream);
                    int len=((DataInputStream)in).readInt();
                    byte[] tmp=new byte[len];
                    ((DataInputStream)in).readFully(tmp, 0, tmp.length);
                    retval=tmp;
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
        final ByteArrayOutputStream out_stream=new ByteArrayOutputStream(512);

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
            else if((type=PRIMITIVE_TYPES.get(obj.getClass())) != null) {
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
                        String str=(String)obj;
                        if(str.length() > Short.MAX_VALUE) {
                            ((DataOutputStream)out).writeBoolean(true);
                            ObjectOutputStream oos=new ObjectOutputStream(out);
                            try {
                                oos.writeObject(str);
                            }
                            finally {
                                oos.close();
                            }
                        }
                        else {
                            ((DataOutputStream)out).writeBoolean(false);
                            ((DataOutputStream)out).writeUTF(str);
                        }
                        break;
                    case TYPE_BYTEARRAY:
                        byte[] buf=(byte[])obj;
                        ((DataOutputStream)out).writeInt(buf.length);
                        out.write(buf, 0, buf.length);
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
        return result;
    }



    public static void objectToStream(Object obj, DataOutputStream out) throws Exception {
        if(obj == null) {
            out.write(TYPE_NULL);
            return;
        }

        Byte type;
        try {
            if(obj instanceof Streamable) {  // use Streamable if we can
                out.write(TYPE_STREAMABLE);
                writeGenericStreamable((Streamable)obj, out);
            }
            else if((type=PRIMITIVE_TYPES.get(obj.getClass())) != null) {
                out.write(type.byteValue());
                switch(type.byteValue()) {
                    case TYPE_BOOLEAN:
                        out.writeBoolean(((Boolean)obj).booleanValue());
                        break;
                    case TYPE_BYTE:
                        out.writeByte(((Byte)obj).byteValue());
                        break;
                    case TYPE_CHAR:
                        out.writeChar(((Character)obj).charValue());
                        break;
                    case TYPE_DOUBLE:
                        out.writeDouble(((Double)obj).doubleValue());
                        break;
                    case TYPE_FLOAT:
                        out.writeFloat(((Float)obj).floatValue());
                        break;
                    case TYPE_INT:
                        out.writeInt(((Integer)obj).intValue());
                        break;
                    case TYPE_LONG:
                        out.writeLong(((Long)obj).longValue());
                        break;
                    case TYPE_SHORT:
                        out.writeShort(((Short)obj).shortValue());
                        break;
                    case TYPE_STRING:
                        String str=(String)obj;
                        if(str.length() > Short.MAX_VALUE) {
                            out.writeBoolean(true);
                            ObjectOutputStream oos=new ObjectOutputStream(out);
                            try {
                                oos.writeObject(str);
                            }
                            finally {
                                oos.close();
                            }
                        }
                        else {
                            out.writeBoolean(false);
                            out.writeUTF(str);
                        }
                        break;
                    case TYPE_BYTEARRAY:
                        byte[] buf=(byte[])obj;
                        out.writeInt(buf.length);
                        out.write(buf, 0, buf.length);
                        break;
                    default:
                        throw new IllegalArgumentException("type " + type + " is invalid");
                }
            }
            else { // will throw an exception if object is not serializable
                out.write(TYPE_SERIALIZABLE);
                ObjectOutputStream tmp=new ObjectOutputStream(out);
                tmp.writeObject(obj);
            }
        }
        finally {
            Util.close(out);
        }
    }



    public static Object objectFromStream(DataInputStream in) throws Exception {
        if(in == null) return null;
        Object retval=null;
        byte b=(byte)in.read();

        switch(b) {
            case TYPE_NULL:
                return null;
            case TYPE_STREAMABLE:
                retval=readGenericStreamable(in);
                break;
            case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                ObjectInputStream tmp=new ObjectInputStream(in);
                retval=tmp.readObject();
                break;
            case TYPE_BOOLEAN:
                retval=Boolean.valueOf(in.readBoolean());
                break;
            case TYPE_BYTE:
                retval=Byte.valueOf(in.readByte());
                break;
            case TYPE_CHAR:
                retval=Character.valueOf(in.readChar());
                break;
            case TYPE_DOUBLE:
                retval=Double.valueOf(in.readDouble());
                break;
            case TYPE_FLOAT:
                retval=Float.valueOf(in.readFloat());
                break;
            case TYPE_INT:
                retval=Integer.valueOf(in.readInt());
                break;
            case TYPE_LONG:
                retval=Long.valueOf(in.readLong());
                break;
            case TYPE_SHORT:
                retval=Short.valueOf(in.readShort());
                break;
            case TYPE_STRING:
                if(in.readBoolean()) { // large string
                    ObjectInputStream ois=new ObjectInputStream(in);
                    try {
                        retval=ois.readObject();
                    }
                    finally {
                        ois.close();
                    }
                }
                else {
                    retval=in.readUTF();
                }
                break;
            case TYPE_BYTEARRAY:
                int len=in.readInt();
                byte[] tmpbuf=new byte[len];
                in.readFully(tmpbuf, 0, tmpbuf.length);
                retval=tmpbuf;
                break;
            default:
                throw new IllegalArgumentException("type " + b + " is invalid");
        }
        return retval;
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
            ObjectInputStream in=new ObjectInputStream(in_stream); // changed Nov 29 2004 (bela)
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
        final ByteArrayOutputStream out_stream=new ByteArrayOutputStream(512);
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
        return retval;
    }

    public static byte[] streamableToByteBuffer(Streamable obj) throws Exception {
        byte[] result=null;
        final ByteArrayOutputStream out_stream=new ByteArrayOutputStream(512);
        DataOutputStream out=new DataOutputStream(out_stream);
        obj.writeTo(out);
        result=out_stream.toByteArray();
        out.close();
        return result;
    }


    public static byte[] collectionToByteBuffer(Collection<Address> c) throws Exception {
        byte[] result=null;
        final ByteArrayOutputStream out_stream=new ByteArrayOutputStream(512);
        DataOutputStream out=new DataOutputStream(out_stream);
        Util.writeAddresses(c, out);
        result=out_stream.toByteArray();
        out.close();
        return result;
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


    public static void writeView(View view, DataOutputStream out) throws IOException {
        if(view == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeBoolean(view instanceof MergeView);
        view.writeTo(out);
    }

    public static View readView(DataInputStream in) throws IOException, InstantiationException, IllegalAccessException {
        if(in.readBoolean() == false)
            return null;
        boolean isMergeView=in.readBoolean();
        View view;
        if(isMergeView)
            view=new MergeView();
        else
            view=new View();
        view.readFrom(in);
        return view;
    }

    public static void writeAddress(Address addr, DataOutputStream out) throws IOException {
        byte flags=0;
        boolean streamable_addr=true;

        if(addr == null) {
            flags=Util.setFlag(flags, Address.NULL);
            out.writeByte(flags);
            return;
        }
        if(addr instanceof UUID) {
            flags=Util.setFlag(flags, Address.UUID_ADDR);
        }
        else if(addr instanceof IpAddress) {
            flags=Util.setFlag(flags, Address.IP_ADDR);
        }
        else {
            streamable_addr=false;
        }

        out.writeByte(flags);
        if(streamable_addr)
            addr.writeTo(out);
        else
            writeOtherAddress(addr, out);
    }

    public static Address readAddress(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        byte flags=in.readByte();
        if(Util.isFlagSet(flags, Address.NULL))
            return null;

        Address addr;
        if(Util.isFlagSet(flags, Address.UUID_ADDR)) {
            addr=new UUID();
            addr.readFrom(in);
        }
        else if(Util.isFlagSet(flags, Address.IP_ADDR)) {
            addr=new IpAddress();
            addr.readFrom(in);
        }
        else {
            addr=readOtherAddress(in);
        }
        return addr;
    }

    public static int size(Address addr) {
        int retval=Global.BYTE_SIZE; // flags
        if(addr != null) {
            if(addr instanceof UUID || addr instanceof IpAddress)
                retval+=addr.size();
            else {
                retval+=Global.SHORT_SIZE; // magic number
                retval+=addr.size();
            }
        }
        return retval;
    }

    public static int size(View view) {
        int retval=Global.BYTE_SIZE; // presence
        if(view != null)
            retval+=view.serializedSize() + Global.BYTE_SIZE; // merge view or regular view
        return retval;
    }

    private static Address readOtherAddress(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        short magic_number=in.readShort();
        Class cl=ClassConfigurator.get(magic_number);
        if(cl == null)
            throw new RuntimeException("class for magic number " + magic_number + " not found");
        Address addr=(Address)cl.newInstance();
        addr.readFrom(in);
        return addr;
    }

    private static void writeOtherAddress(Address addr, DataOutputStream out) throws IOException {
        short magic_number=ClassConfigurator.getMagicNumber(addr.getClass());

        // write the class info
        if(magic_number == -1)
            throw new RuntimeException("magic number " + magic_number + " not found");

        out.writeShort(magic_number);
        addr.writeTo(out);
    }

    /**
     * Writes a Vector of Addresses. Can contain 65K addresses at most
     * @param v A Collection<Address>
     * @param out
     * @throws IOException
     */
    public static void writeAddresses(Collection<? extends Address> v, DataOutputStream out) throws IOException {
        if(v == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(v.size());
        for(Address addr: v) {
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
    public static Collection<? extends Address> readAddresses(DataInputStream in, Class cl) throws IOException, IllegalAccessException, InstantiationException {
        short length=in.readShort();
        if(length < 0) return null;
        Collection<Address> retval=(Collection<Address>)cl.newInstance();
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
    public static long size(Collection<? extends Address> addrs) {
        int retval=Global.SHORT_SIZE; // number of elements
        if(addrs != null && !addrs.isEmpty()) {
            Address addr=addrs.iterator().next();
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
        short magic_number;
        String classname;

        if(obj == null) {
            out.write(0);
            return;
        }

        out.write(1);
        magic_number=ClassConfigurator.getMagicNumber(obj.getClass());
        // write the magic number or the class name
        if(magic_number == -1) {
            out.writeBoolean(false);
            classname=obj.getClass().getName();
            out.writeUTF(classname);
        }
        else {
            out.writeBoolean(true);
            out.writeShort(magic_number);
        }

        // write the contents
        obj.writeTo(out);
    }



    public static Streamable readGenericStreamable(DataInputStream in) throws IOException {
        Streamable retval=null;
        int b=in.read();
        if(b == 0)
            return null;

        boolean use_magic_number=in.readBoolean();
        String classname;
        Class clazz;

        try {
            if(use_magic_number) {
                short magic_number=in.readShort();
                clazz=ClassConfigurator.get(magic_number);
                if (clazz==null) {
                   throw new ClassNotFoundException("Class for magic number "+magic_number+" cannot be found.");
                }
            }
            else {
                classname=in.readUTF();
                clazz=ClassConfigurator.get(classname);
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

    public static void writeAsciiString(String str, DataOutputStream out) throws IOException {
        if(str == null) {
            out.write(-1);
            return;
        }
        int length=str.length();
        if(length > Byte.MAX_VALUE)
            throw new IllegalArgumentException("string is > " + Byte.MAX_VALUE);
        out.write(length);
        out.writeBytes(str);
    }

    public static String readAsciiString(DataInputStream in) throws IOException {
        byte length=(byte)in.read();
        if(length == -1)
            return null;
        byte[] tmp=new byte[length];
        in.readFully(tmp, 0, tmp.length);
        return new String(tmp, 0, tmp.length);
    }


    public static String parseString(DataInputStream in) {
        return parseString(in, false);
    }

    public static String parseString(DataInputStream in, boolean break_on_newline) {
        StringBuilder sb=new StringBuilder();
        int ch;

        // read white space
        while(true) {
            try {
                ch=in.read();
                if(ch == -1) {
                    return null; // eof
                }
                if(Character.isWhitespace(ch)) {
                    if(break_on_newline && ch == '\n')
                        return null;
                }
                else {
                    sb.append((char)ch);
                    break;
                }
            }
            catch(IOException e) {
                break;
            }
        }

        while(true) {
            try {
                ch=in.read();
                if(ch == -1)
                    break;
                if(Character.isWhitespace(ch))
                    break;
                else {
                    sb.append((char)ch);
                }
            }
            catch(IOException e) {
                break;
            }
        }

        return sb.toString();
    }

    public static String readStringFromStdin(String message) throws Exception {
        System.out.print(message);
        System.out.flush();
        System.in.skip(System.in.available());
        BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
        return reader.readLine().trim();
    }

    public static long readLongFromStdin(String message) throws Exception {
        String tmp=readStringFromStdin(message);
        return Long.parseLong(tmp);
    }

    public static int readIntFromStdin(String message) throws Exception {
        String tmp=readStringFromStdin(message);
        return Integer.parseInt(tmp);
    }


    public static void writeByteBuffer(byte[] buf, DataOutputStream out) throws IOException {
        writeByteBuffer(buf, 0, buf.length, out);
    }

     public static void writeByteBuffer(byte[] buf, int offset, int length, DataOutputStream out) throws IOException {
        if(buf != null) {
            out.write(1);
            out.writeInt(length);
            out.write(buf, offset, length);
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
            in.readFully(buf, 0, buf.length);
            return buf;
        }
        return null;
    }


    public static Buffer messageToByteBuffer(Message msg) throws IOException {
        ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new DataOutputStream(output);

        out.writeBoolean(msg != null);
        if(msg != null)
            msg.writeTo(out);
        out.flush();
        Buffer retval=new Buffer(output.getRawBuffer(), 0, output.size());
        out.close();
        output.close();
        return retval;
    }

    public static Message byteBufferToMessage(byte[] buffer, int offset, int length) throws Exception {
        ByteArrayInputStream input=new ByteArrayInputStream(buffer, offset, length);
        DataInputStream in=new DataInputStream(input);

        if(!in.readBoolean())
            return null;

        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(in);
        return msg;
    }



    /**
       * Marshalls a list of messages.
       * @param xmit_list LinkedList<Message>
       * @return Buffer
       * @throws IOException
       */
    public static Buffer msgListToByteBuffer(List<Message> xmit_list) throws IOException {
        ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new DataOutputStream(output);
        Buffer retval=null;

        out.writeInt(xmit_list.size());
        for(Message msg: xmit_list) {
            msg.writeTo(out);
        }
        out.flush();
        retval=new Buffer(output.getRawBuffer(), 0, output.size());
        out.close();
        output.close();
        return retval;
    }

    public static List<Message> byteBufferToMessageList(byte[] buffer, int offset, int length) throws Exception {
        List<Message>  retval=null;
        ByteArrayInputStream input=new ByteArrayInputStream(buffer, offset, length);
        DataInputStream in=new DataInputStream(input);
        int size=in.readInt();

        if(size == 0)
            return null;

        Message msg;
        retval=new LinkedList<Message>();
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

    public static boolean sameViewId(ViewId one, ViewId two) {
        return one.getId() == two.getId() && one.getCoordAddress().equals(two.getCoordAddress());
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
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    public static void sleep(long timeout, int nanos) {
        try {
            Thread.sleep(timeout,nanos);
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
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


    public static int keyPress(String msg) {
        System.out.println(msg);

        try {
            int ret=System.in.read();
            System.in.skip(System.in.available());
            return ret;
        }
        catch(IOException e) {
            return 0;
        }
    }






    /** Returns a random value in the range [1 - range] */
    public static long random(long range) {
        return (long)((Math.random() * range) % range) + 1;
    }


    /** Sleeps between 1 and timeout milliseconds, chosen randomly. Timeout must be > 1 */
    public static void sleepRandom(long timeout) {
        if(timeout <= 0) {
            return;
        }

        long r=(int)((Math.random() * 100000) % timeout) + 1;
        sleep(r);
    }

    /** Sleeps between floor and ceiling milliseconds, chosen randomly */
    public static void sleepRandom(long floor, long ceiling) {
        if(ceiling - floor<= 0) {
            return;
        }
        long diff = ceiling - floor;
        long r=(int)((Math.random() * 100000) % diff) + floor;
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


    public static String dumpThreads() {
        StringBuilder sb=new StringBuilder();
        ThreadMXBean bean=ManagementFactory.getThreadMXBean();
        long[] ids=bean.getAllThreadIds();
        ThreadInfo[] threads=bean.getThreadInfo(ids, 20);
        for(int i=0; i < threads.length; i++) {
            ThreadInfo info=threads[i];
            if(info == null)
                continue;
            sb.append(info.getThreadName()).append(":\n");
            StackTraceElement[] stack_trace=info.getStackTrace();
            for(int j=0; j < stack_trace.length; j++) {
                StackTraceElement el=stack_trace[j];
                sb.append("    at ").append(el.getClassName()).append(".").append(el.getMethodName());
                sb.append("(").append(el.getFileName()).append(":").append(el.getLineNumber()).append(")");
                sb.append("\n");
            }
            sb.append("\n\n");
        }
        return sb.toString();
    }

    public static boolean interruptAndWaitToDie(Thread t) {
		return interruptAndWaitToDie(t, Global.THREAD_SHUTDOWN_WAIT_TIME);
	}

    public static boolean interruptAndWaitToDie(Thread t, long timeout) {
        if(t == null)
            throw new IllegalArgumentException("Thread can not be null");
        t.interrupt(); // interrupts the sleep()
        try {
            t.join(timeout);
        }
        catch(InterruptedException e){
            Thread.currentThread().interrupt(); // set interrupt flag again
        }
        return t.isAlive();
	}


    /**
	 * Debugging method used to dump the content of a protocol queue in a
	 * condensed form. Useful to follow the evolution of the queue's content in
	 * time.
	 */
    public static String dumpQueue(Queue q) {
        StringBuilder sb=new StringBuilder();
        LinkedList values=q.values();
        if(values.isEmpty()) {
            sb.append("empty");
        }
        else {
            for(Object o: values) {
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
                        Map<String,Header> headers=new HashMap<String,Header>(m.getHeaders());
                        for(Map.Entry<String,Header> entry: headers.entrySet()) {
                            String headerKey=entry.getKey();
                            Header value=entry.getValue();
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
                            s+=" ";
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

    public static String mapToString(Map<? extends Object,? extends Object> map) {
        if(map == null)
            return "null";
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<? extends Object,? extends Object> entry: map.entrySet()) {
            Object key=entry.getKey();
            Object val=entry.getValue();
            sb.append(key).append("=");
            if(val == null)
                sb.append("null");
            else
                sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
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


    public static long readBytesLong(String input) {
        Tuple<String,Long> tuple=readBytes(input);
        double num=Double.parseDouble(tuple.getVal1());
        return (long)(num * tuple.getVal2());
    }

    public static int readBytesInteger(String input) {
        Tuple<String,Long> tuple=readBytes(input);
        double num=Double.parseDouble(tuple.getVal1());
        return (int)(num * tuple.getVal2());
    }

    public static double readBytesDouble(String input) {
        Tuple<String,Long> tuple=readBytes(input);
        double num=Double.parseDouble(tuple.getVal1());
        return num * tuple.getVal2();
    }

    private static Tuple<String,Long> readBytes(String input) {
        input=input.trim().toLowerCase();

        int  index=-1;
        long factor=1;

        if((index=input.indexOf("k")) != -1)
            factor=1000;
        else if((index=input.indexOf("kb")) != -1)
            factor=1000;
        else if((index=input.indexOf("m")) != -1)
            factor=1000000;
        else if((index=input.indexOf("mb")) != -1)
            factor=1000000;
        else if((index=input.indexOf("g")) != -1)
            factor=1000000000;
        else if((index=input.indexOf("gb")) != -1)
            factor=1000000000;

        String str=index != -1? input.substring(0, index) : input;
        return new Tuple<String,Long>(str, factor);
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


    public static List<String> split(String input, int separator) {
        List<String> retval=new ArrayList<String>();
        if(input == null)
            return retval;
        int index=0, end;
        while(true) {
            index=input.indexOf(separator, index);
            if(index == -1)
                break;
            index++;
            end=input.indexOf(separator, index);
            if(end == -1)
                retval.add(input.substring(index));
            else
                retval.add(input.substring(index, end));
        }
        return retval;
    }


   /* public static String[] components(String path, String separator) {
        if(path == null || path.length() == 0)
            return null;
        String[] tmp=path.split(separator + "+"); // multiple separators could be present
        if(tmp == null)
            return null;
        if(tmp.length == 0)
            return null;

        if(tmp[0].length() == 0) {
            tmp[0]=separator;
            if(tmp.length > 1) {
                String[] retval=new String[tmp.length -1];
                retval[0]=tmp[0] + tmp[1];
                System.arraycopy(tmp, 2, retval, 1, tmp.length-2);
                return retval;
            }
            return tmp;
        }
        return tmp;
    }*/


     public static String[] components(String path, String separator) {
        if(path == null || path.length() == 0)
            return null;
        String[] tmp=path.split(separator + "+"); // multiple separators could be present
        if(tmp == null)
            return null;
        if(tmp.length == 0)
            return null;

        if(tmp[0].length() == 0)
            tmp[0]=separator;
        return tmp;
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
    public static List<Range> computeFragOffsets(int offset, int length, int frag_size) {
        List<Range>   retval=new ArrayList<Range>();
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

    public static List<Range> computeFragOffsets(byte[] buf, int frag_size) {
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

    public static <T> String printListWithDelimiter(Collection<T> list, String delimiter) {
        boolean first=true;
        StringBuilder sb=new StringBuilder();
        for(T el: list) {
            if(first) {
                first=false;
            }
            else {
                sb.append(delimiter);
            }
            sb.append(el);
        }
        return sb.toString();
    }


    public static <T> String printMapWithDelimiter(Map<T,T> map, String delimiter) {
        boolean first=true;
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<T,T> entry: map.entrySet()) {
            if(first)
                first=false;
            else
                sb.append(delimiter);
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }




    public static String array2String(long[] array) {
        StringBuilder ret=new StringBuilder("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }

        ret.append(']');
        return ret.toString();
    }

    public static String array2String(short[] array) {
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
        return Arrays.toString(array);
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
     * Returns a list of members which left from view one to two
     * @param one
     * @param two
     * @return
     */
    public static List<Address> leftMembers(View one, View two) {
        if(one == null || two == null)
            return null;
        List<Address> retval=new ArrayList<Address>(one.getMembers());
        retval.removeAll(two.getMembers());
        return retval;
    }

    public static List<Address> leftMembers(Collection<Address> old_list, Collection<Address> new_list) {
        if(old_list == null || new_list == null)
            return null;
        List<Address> retval=new ArrayList<Address>(old_list);
        retval.removeAll(new_list);
        return retval;
    }

    public static List<Address> newMembers(List<Address> old_list, List<Address> new_list) {
        if(old_list == null || new_list == null)
            return null;
        List<Address> retval=new ArrayList<Address>(new_list);
        retval.removeAll(old_list);
        return retval;
    }


    /**
     * Selects a random subset of members according to subset_percentage and returns them.
     * Picks no member twice from the same membership. If the percentage is smaller than 1 -> picks 1 member.
     */
    public static Vector<Address> pickSubset(Vector<Address> members, double subset_percentage) {
        Vector<Address> ret=new Vector<Address>(), tmp_mbrs;
        int num_mbrs=members.size(), subset_size, index;

        if(num_mbrs == 0) return ret;
        subset_size=(int)Math.ceil(num_mbrs * subset_percentage);

        tmp_mbrs=(Vector<Address>)members.clone();

        for(int i=subset_size; i > 0 && !tmp_mbrs.isEmpty(); i--) {
            index=(int)((Math.random() * num_mbrs) % tmp_mbrs.size());
            ret.addElement(tmp_mbrs.elementAt(index));
            tmp_mbrs.removeElementAt(index);
        }

        return ret;
    }



    public static boolean containsViewId(Collection<View> views, ViewId vid) {
        for(View view: views) {
            ViewId tmp=view.getVid();
            if(Util.sameViewId(vid, tmp))
                return true;
        }
        return false;
    }



    /**
     * Determines the members which take part in a merge. The resulting list consists of all merge coordinators
     * and members outside a merge partition, e.g. for views A={B,A,C}, B={B,C} and C={B,C}, the merge coordinator
     * is B, but the merge participants are B and A.
     * @param map
     * @return
     */
    public static Collection<Address> determineMergeParticipants(Map<Address,View> map) {
        Set<Address> coords=new HashSet<Address>();
        Set<Address> all_addrs=new HashSet<Address>();

        if(map == null)
            return Collections.emptyList();

        for(View view: map.values())
            all_addrs.addAll(view.getMembers());

        for(View view: map.values()) {
            Address coord=view.getCreator();
            if(coord != null)
                coords.add(coord);
        }

        for(Address coord: coords) {
            View view=map.get(coord);
            Collection<Address> mbrs=view != null? view.getMembers() : null;
            if(mbrs != null)
                all_addrs.removeAll(mbrs);
        }
        coords.addAll(all_addrs);
        return coords;
    }


    /**
     * This is the same or a subset of {@link #determineMergeParticipants(java.util.Map)} and contains only members
     * which are currently sub-partition coordinators.
     * @param map
     * @return
     */
    public static Collection<Address> determineMergeCoords(Map<Address,View> map) {
        Set<Address> retval=new HashSet<Address>();
        if(map != null) {
            for(View view: map.values()) {
                Address coord=view.getCreator();
                if(coord != null)
                    retval.add(coord);
            }
        }
        return retval;
    }



    public static Object pickRandomElement(List list) {
        if(list == null) return null;
        int size=list.size();
        int index=(int)((Math.random() * size * 10) % size);
        return list.get(index);
    }

    public static Object pickRandomElement(Object[] array) {
        if(array == null) return null;
        int size=array.length;
        int index=(int)((Math.random() * size * 10) % size);
        return array[index];
    }


    public static View createView(Address coord, long id, Address ... members) {
        Vector<Address> mbrs=new Vector<Address>();
        mbrs.addAll(Arrays.asList(members));
        return new View(coord, id, mbrs);
    }


    public static Address createRandomAddress() {
        UUID retval=UUID.randomUUID();
        String name=generateLocalName();
        UUID.add(retval, name);
        return retval;
    }

    /**
     * Returns all members that left between 2 views. All members that are element of old_mbrs but not element of
     * new_mbrs are returned.
     */
    public static Vector<Address> determineLeftMembers(Vector<Address> old_mbrs, Vector<Address> new_mbrs) {
        Vector<Address> retval=new Vector<Address>();
        Address mbr;

        if(old_mbrs == null || new_mbrs == null)
            return retval;

        for(int i=0; i < old_mbrs.size(); i++) {
            mbr=old_mbrs.elementAt(i);
            if(!new_mbrs.contains(mbr))
                retval.addElement(mbr);
        }

        return retval;
    }



    public static String printViews(Collection<View> views) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(View view: views) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(view.getVid());
        }
        return sb.toString();
    }

   public static <T> String print(Collection<T> objs) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(T obj: objs) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(obj);
        }
        return sb.toString();
    }


    public static <T> String print(Map<T,T> map) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(Map.Entry<T,T> entry: map.entrySet()) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }



    public static String printPingData(List<PingData> rsps) {
        StringBuilder sb=new StringBuilder();
        if(rsps != null) {
            int total=rsps.size();
            int servers=0, clients=0, coords=0;
            for(PingData rsp: rsps) {
                if(rsp.isCoord())
                    coords++;
                if(rsp.isServer())
                    servers++;
                else
                    clients++;
            }
            sb.append(total + " total (" + servers + " servers (" + coords + " coord), " + clients + " clients)");
        }
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

    public static int  sizeOf(Streamable inst) {
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


    public static Field[] getAllDeclaredFields(final Class clazz) {
        return getAllDeclaredFieldsWithAnnotations(clazz);
    }

    public static Field[] getAllDeclaredFieldsWithAnnotations(final Class clazz, Class<? extends Annotation> ... annotations) {
        List<Field> list=new ArrayList<Field>(30);
        for(Class curr=clazz; curr != null; curr=curr.getSuperclass()) {
            Field[] fields=curr.getDeclaredFields();
            if(fields != null) {
                for(Field field: fields) {
                    if(annotations != null && annotations.length > 0) {
                        for(Class<? extends Annotation> annotation: annotations) {
                            if(field.isAnnotationPresent(annotation))
                                list.add(field);
                        }
                    }
                    else
                        list.add(field);
                }
            }
        }

        Field[] retval=new Field[list.size()];
        for(int i=0; i < list.size(); i++)
            retval[i]=list.get(i);
        return retval;
    }

    public static Method[] getAllDeclaredMethods(final Class clazz) {
        return getAllDeclaredMethodsWithAnnotations(clazz);
    }

    public static Method[] getAllDeclaredMethodsWithAnnotations(final Class clazz, Class<? extends Annotation> ... annotations) {
        List<Method> list=new ArrayList<Method>(30);
        for(Class curr=clazz; curr != null; curr=curr.getSuperclass()) {
            Method[] methods=curr.getDeclaredMethods();
            if(methods != null) {
                for(Method method: methods) {
                    if(annotations != null && annotations.length > 0) {
                        for(Class<? extends Annotation> annotation: annotations) {
                            if(method.isAnnotationPresent(annotation))
                                list.add(method);
                        }
                    }
                    else
                        list.add(method);
                }
            }
        }

        Method[] retval=new Method[list.size()];
        for(int i=0; i < list.size(); i++)
            retval[i]=list.get(i);
        return retval;
    }

    public static Field getField(final Class clazz, String field_name) {
        if(clazz == null || field_name == null)
            return null;

        Field field=null;
        for(Class curr=clazz; curr != null; curr=curr.getSuperclass()) {
            try {
                return curr.getDeclaredField(field_name);
            }
            catch(NoSuchFieldException e) {
            }
        }
        return field;
    }


    public static InputStream getResourceAsStream(String name, Class clazz) {
        ClassLoader loader;
        InputStream retval=null;

        try {
            loader=Thread.currentThread().getContextClassLoader();
            if(loader != null) {
                retval=loader.getResourceAsStream(name);
                if(retval != null)
                    return retval;
            }
        }
        catch(Throwable t) {
        }

        if(clazz != null) {
            try {
                loader=clazz.getClassLoader();
                if(loader != null) {
                    retval=loader.getResourceAsStream(name);
                    if(retval != null)
                        return retval;
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

        return retval;
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
        Vector<Long> v=new Vector<Long>();
        Long l;
        long[] retval=null;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            l=new Long(tok.nextToken());
            v.addElement(l);
        }
        if(v.isEmpty()) return null;
        retval=new long[v.size()];
        for(int i=0; i < v.size(); i++)
            retval[i]=v.elementAt(i).longValue();
        return retval;
    }

    /** e.g. "bela,jeannette,michelle" --> List{"bela", "jeannette", "michelle"} */
    public static List<String> parseCommaDelimitedStrings(String l) {
        return parseStringList(l, ",");
    }

    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Returns a list of IpAddresses
     */
    public static List<IpAddress> parseCommaDelimitedHosts(String hosts, int port_range) throws UnknownHostException {
        StringTokenizer tok=new StringTokenizer(hosts, ",");
        String t;
        IpAddress addr;
        Set<IpAddress> retval=new HashSet<IpAddress>();

        while(tok.hasMoreTokens()) {
            t=tok.nextToken().trim();
            String host=t.substring(0, t.indexOf('['));
            host=host.trim();
            int port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
            for(int i=port;i < port + port_range;i++) {
                addr=new IpAddress(host, i);
                retval.add(addr);
            }
        }
        return Collections.unmodifiableList(new LinkedList<IpAddress>(retval));
    }


    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of
     * InetSocketAddress
     */
    public static List<InetSocketAddress> parseCommaDelimitedHosts2(String hosts, int port_range)
            throws UnknownHostException {

        StringTokenizer tok=new StringTokenizer(hosts, ",");
        String t;
        InetSocketAddress addr;
        Set<InetSocketAddress> retval=new HashSet<InetSocketAddress>();

        while(tok.hasMoreTokens()) {
            t=tok.nextToken().trim();
            String host=t.substring(0, t.indexOf('['));
            host=host.trim();
            int port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
            for(int i=port;i < port + port_range;i++) {
                addr=new InetSocketAddress(host, i);
                retval.add(addr);
            }
        }
        return Collections.unmodifiableList(new LinkedList<InetSocketAddress>(retval));
   }

    public static List<String> parseStringList(String l, String separator) {
         List<String> tmp=new LinkedList<String>();
         StringTokenizer tok=new StringTokenizer(l, separator);
         String t;

         while(tok.hasMoreTokens()) {
             t=tok.nextToken();
             tmp.add(t.trim());
         }

         return tmp;
     }


    public static String parseString(ByteBuffer buf) {
        return parseString(buf, true);
    }


    public static String parseString(ByteBuffer buf, boolean discard_whitespace) {
        StringBuilder sb=new StringBuilder();
        char ch;

        // read white space
        while(buf.remaining() > 0) {
            ch=(char)buf.get();
            if(!Character.isWhitespace(ch)) {
                buf.position(buf.position() -1);
                break;
            }
        }

        if(buf.remaining() == 0)
            return null;

        while(buf.remaining() > 0) {
            ch=(char)buf.get();
            if(!Character.isWhitespace(ch)) {
                sb.append(ch);
            }
            else
                break;
        }

        // read white space
        if(discard_whitespace) {
            while(buf.remaining() > 0) {
                ch=(char)buf.get();
                if(!Character.isWhitespace(ch)) {
                    buf.position(buf.position() -1);
                    break;
                }
            }
        }
        return sb.toString();
    }

    public static int readNewLine(ByteBuffer buf) {
        char ch;
        int num=0;

        while(buf.remaining() > 0) {
            ch=(char)buf.get();
            num++;
            if(ch == '\n')
                break;
        }
        return num;
    }


    /**
     * Reads and discards all characters from the input stream until a \r\n or EOF is encountered
     * @param in
     * @return
     */
    public static int discardUntilNewLine(InputStream in) {
        int ch;
        int num=0;

        while(true) {
            try {
                ch=in.read();
                if(ch == -1)
                    break;
                num++;
                if(ch == '\n')
                    break;
            }
            catch(IOException e) {
                break;
            }
        }
        return num;
    }

    /**
     * Reads a line of text.  A line is considered to be terminated by any one
     * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
     * followed immediately by a linefeed.
     *
     *  @return     A String containing the contents of the line, not including
     *             any line-termination characters, or null if the end of the
     *             stream has been reached
     *
     * @exception  IOException  If an I/O error occurs
     */
    public static String readLine(InputStream in) throws IOException {
        StringBuilder sb=new StringBuilder(35);
        int ch;

        while(true) {
            ch=in.read();
            if(ch == -1)
                return null;
            if(ch == '\r') {
                ;
            }
            else {
                if(ch == '\n')
                    break;
                else {
                    sb.append((char)ch);
                }
            }
        }
        return sb.toString();
    }


    public static void writeString(ByteBuffer buf, String s) {
        for(int i=0; i < s.length(); i++)
            buf.put((byte)s.charAt(i));
    }


    /**
     *
     * @param s
     * @return List<NetworkInterface>
     */
    public static List<NetworkInterface> parseInterfaceList(String s) throws Exception {
        List<NetworkInterface> interfaces=new ArrayList<NetworkInterface>(10);
        if(s == null)
            return null;

        StringTokenizer tok=new StringTokenizer(s, ",");
        String interface_name;
        NetworkInterface intf;

        while(tok.hasMoreTokens()) {
            interface_name=tok.nextToken();

            // try by name first (e.g. (eth0")
            intf=NetworkInterface.getByName(interface_name);

            // next try by IP address or symbolic name
            if(intf == null)
                intf=NetworkInterface.getByInetAddress(InetAddress.getByName(interface_name));

            if(intf == null)
                throw new Exception("interface " + interface_name + " not found");
            if(!interfaces.contains(intf)) {
                interfaces.add(intf);
            }
        }
        return interfaces;
    }


    public static String print(List<NetworkInterface> interfaces) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;

        for(NetworkInterface intf: interfaces) {
            if(first) {
                first=false;
            }
            else {
                sb.append(", ");
            }
            sb.append(intf.getName());
        }
        return sb.toString();
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

    public static boolean startFlush(Channel c, List<Address> flushParticipants, int numberOfAttempts,  long randomSleepTimeoutFloor,long randomSleepTimeoutCeiling) {
    	boolean successfulFlush = false;
        int attemptCount = 0;
        while(attemptCount < numberOfAttempts){
        	successfulFlush = c.startFlush(flushParticipants, false);
        	if(successfulFlush)
        		break;
        	Util.sleepRandom(randomSleepTimeoutFloor,randomSleepTimeoutCeiling);
        	attemptCount++;
        }
        return successfulFlush;
    }

    public static boolean startFlush(Channel c, List<Address> flushParticipants) {
    	return startFlush(c,flushParticipants,4,1000,5000);
    }

    public static boolean startFlush(Channel c, int numberOfAttempts, long randomSleepTimeoutFloor,long randomSleepTimeoutCeiling) {
    	boolean successfulFlush = false;
        int attemptCount = 0;
        while(attemptCount < numberOfAttempts){
        	successfulFlush = c.startFlush(false);
        	if(successfulFlush)
        		break;
        	Util.sleepRandom(randomSleepTimeoutFloor,randomSleepTimeoutCeiling);
        	attemptCount++;
        }
        return successfulFlush;
    }

    public static boolean startFlush(Channel c) {
    	return startFlush(c,4,1000,5000);
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

    public static String generateLocalName() {
        String retval=null;
        try {
            retval=InetAddress.getLocalHost().getHostName();
        }
        catch(UnknownHostException e) {
            retval="localhost";
        }

        long counter=Util.random(Short.MAX_VALUE *2);
        return retval + "-" + counter;
    }




    public synchronized static short incrCounter() {
        short retval=COUNTER++;
        if(COUNTER >= Short.MAX_VALUE)
            COUNTER=1;
        return retval;
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
            }
        }
        return sock; // will never be reached, but the stupid compiler didn't figure it out...
    }


    public static MulticastSocket createMulticastSocket(int port) throws IOException {
        return createMulticastSocket(null, port, null);
    }

    public static MulticastSocket createMulticastSocket(InetAddress mcast_addr, int port, Log log) throws IOException {
        if(mcast_addr != null && !mcast_addr.isMulticastAddress())
            throw new IllegalArgumentException("mcast_addr (" + mcast_addr + ") is not a valid multicast address");

        SocketAddress saddr=new InetSocketAddress(mcast_addr, port);
        MulticastSocket retval=null;

        try {
            retval=new MulticastSocket(saddr);
        }
        catch(IOException ex) {
            if(log != null && log.isWarnEnabled()) {
                StringBuilder sb=new StringBuilder();
                String type=mcast_addr != null ? mcast_addr instanceof Inet4Address? "IPv4" : "IPv6" : "n/a";
                sb.append("could not bind to " + mcast_addr + " (" + type + " address)");
                sb.append("; make sure your mcast_addr is of the same type as the preferred IP stack (IPv4 or IPv6)");
                sb.append(" by checking the value of the system properties java.net.preferIPv4Stack and java.net.preferIPv6Addresses.");                
                sb.append("\nWill ignore mcast_addr, but this may lead to cross talking " +
                        "(see http://www.jboss.org/community/docs/DOC-9469 for details). ");
                sb.append("\nException was: " + ex);
                log.warn(sb.toString());
            }
        }
        if(retval == null)
            retval=new MulticastSocket(port);
        return retval;
    }


    /**
     * Returns the address of the interface to use defined by bind_addr and bind_interface
     * @param props
     * @return
     * @throws UnknownHostException
     * @throws SocketException
     */
    public static InetAddress getBindAddress(Properties props) throws UnknownHostException, SocketException {
    	return getBindAddress(props, StackType.IPv4);
    }
    
    public static InetAddress getBindAddress(Properties props, StackType ip_version) throws UnknownHostException, SocketException {

    	// determine the desired values for bind_addr_str and bind_interface_str
    	boolean ignore_systemprops=Util.isBindAddressPropertyIgnored();
    	String bind_addr_str =Util.getProperty(new String[]{Global.BIND_ADDR, Global.BIND_ADDR_OLD}, props, "bind_addr",
    			ignore_systemprops, null);
    	String bind_interface_str =Util.getProperty(new String[]{Global.BIND_INTERFACE, null}, props, "bind_interface",
    			ignore_systemprops, null);
    	
    	InetAddress bind_addr=null;
    	NetworkInterface bind_intf=null ;

    	// 1. if bind_addr_str specified, get bind_addr and check version
    	if(bind_addr_str != null) {
    		bind_addr=InetAddress.getByName(bind_addr_str);

            // check that bind_addr_host has correct IP version
            boolean hasCorrectVersion = ((bind_addr instanceof Inet4Address && ip_version == StackType.IPv4) ||
                    (bind_addr instanceof Inet6Address && ip_version == StackType.IPv6)) ;
            if (!hasCorrectVersion)
                throw new IllegalArgumentException("bind_addr " + bind_addr_str + " has incorrect IP version") ;
        }

    	// 2. if bind_interface_str specified, get interface and check that it has correct version
    	if(bind_interface_str != null) {
    		bind_intf=NetworkInterface.getByName(bind_interface_str);
    		if(bind_intf != null) {

                // check that the interface supports the IP version
                boolean supportsVersion = interfaceHasIPAddresses(bind_intf, ip_version) ;
                if (!supportsVersion)
                    throw new IllegalArgumentException("bind_interface " + bind_interface_str + " has incorrect IP version") ;
            }
            else {
                // (bind_intf == null)
    			throw new UnknownHostException("network interface " + bind_interface_str + " not found");
    		}
    	}

    	// 3. intf and bind_addr are both are specified, bind_addr needs to be on intf
    	if (bind_intf != null && bind_addr != null) {

    		boolean hasAddress = false ;

    		// get all the InetAddresses defined on the interface
    		Enumeration addresses = bind_intf.getInetAddresses() ;

    		while (addresses != null && addresses.hasMoreElements()) {
    			// get the next InetAddress for the current interface
    			InetAddress address = (InetAddress) addresses.nextElement() ;

    			// check if address is on interface
    			if (bind_addr.equals(address)) { 
    				hasAddress = true ;
    				break ;
    			}
    		}

    		if (!hasAddress) {
    			throw new IllegalArgumentException("network interface " + bind_interface_str + " does not contain address " + bind_addr_str);
    		}

    	}
    	// 4. if only interface is specified, get first non-loopback address on that interface, 
    	else if (bind_intf != null) {
            bind_addr = getFirstNonLoopbackAddress(bind_intf, ip_version) ;
    	}
    	// 5. if neither bind address nor bind interface is specified, get the first non-loopback
    	// address on any interface
    	else if (bind_addr == null) {
    		bind_addr = getFirstNonLoopbackAddress(ip_version) ;
    	}

    	// if we reach here, if bind_addr == null, we have tried to obtain a bind_addr but were not successful
    	// in such a case, using a loopback address of the correct version is our only option

    	boolean localhost = false;
    	if (bind_addr == null) {
    		bind_addr = getLocalhost(ip_version);
    		localhost = true;
    	}

    	//http://jira.jboss.org/jira/browse/JGRP-739
    	//check all bind_address against NetworkInterface.getByInetAddress() to see if it exists on the machine
    	//in some Linux setups NetworkInterface.getByInetAddress(InetAddress.getLocalHost()) returns null, so skip
    	//the check in that case
    	if(!localhost && NetworkInterface.getByInetAddress(bind_addr) == null) {
    		throw new UnknownHostException("Invalid bind address " + bind_addr);
    	}

    	if(props != null) {
    		props.remove("bind_addr");
    		props.remove("bind_interface");
    	}
    	return bind_addr;
    }
    
    /**
     * Method used by PropertyConverters.BindInterface to check that a bind_address is
     * consistent with a specified interface 
     * 
     * Idea:
     * 1. We are passed a bind_addr, which may be null
     * 2. If non-null, check that bind_addr is on bind_interface - if not, throw exception, 
     * otherwise, return the original bind_addr
     * 3. If null, get first non-loopback address on bind_interface, using stack preference to
     * get the IP version. If no non-loopback address, then just return null (i.e. the
     * bind_interface did not influence the decision).
     * 
     */
    public static InetAddress validateBindAddressFromInterface(InetAddress bind_addr, String bind_interface_str) throws UnknownHostException, SocketException {
    	
    	NetworkInterface bind_intf=null ;

    	// 1. if bind_interface_str is null, or empty, no constraint on bind_addr
    	if (bind_interface_str == null || bind_interface_str.trim().length() == 0)
    		return bind_addr;
    	
    	// 2. get the preferred IP version for the JVM - it will be IPv4 or IPv6 
    	StackType ip_version = getIpStackType();
    	
    	// 3. if bind_interface_str specified, get interface and check that it has correct version
    	bind_intf=NetworkInterface.getByName(bind_interface_str);
    	if(bind_intf != null) {
            // check that the interface supports the IP version
            boolean supportsVersion = interfaceHasIPAddresses(bind_intf, ip_version) ;
            if (!supportsVersion)
                throw new IllegalArgumentException("bind_interface " + bind_interface_str + " has incorrect IP version") ;
        }
        else {
    		// (bind_intf == null)
    		throw new UnknownHostException("network interface " + bind_interface_str + " not found");
    	}

    	// 3. intf and bind_addr are both are specified, bind_addr needs to be on intf
    	if (bind_addr != null) {

    		boolean hasAddress = false ;

    		// get all the InetAddresses defined on the interface
    		Enumeration addresses = bind_intf.getInetAddresses() ;

    		while (addresses != null && addresses.hasMoreElements()) {
    			// get the next InetAddress for the current interface
    			InetAddress address = (InetAddress) addresses.nextElement() ;

    			// check if address is on interface
    			if (bind_addr.equals(address)) { 
    				hasAddress = true ;
    				break ;
    			}
    		}

    		if (!hasAddress) {
    			String bind_addr_str = bind_addr.getHostAddress();
    			throw new IllegalArgumentException("network interface " + bind_interface_str + " does not contain address " + bind_addr_str);
    		}

    	}
    	// 4. if only interface is specified, get first non-loopback address on that interface, 
    	else {
    		bind_addr = getFirstNonLoopbackAddress(bind_intf, ip_version) ;
    	}


    	//http://jira.jboss.org/jira/browse/JGRP-739
    	//check all bind_address against NetworkInterface.getByInetAddress() to see if it exists on the machine
    	//in some Linux setups NetworkInterface.getByInetAddress(InetAddress.getLocalHost()) returns null, so skip
    	//the check in that case
    	if(bind_addr != null && NetworkInterface.getByInetAddress(bind_addr) == null) {
    		throw new UnknownHostException("Invalid bind address " + bind_addr);
    	}
    	
    	// if bind_addr == null, we have tried to obtain a bind_addr but were not successful
    	// in such a case, return the original value of null so the default will be applied

    	return bind_addr;
    }


    public static boolean checkForLinux() {
        return checkForPresence("os.name", "linux");
    }

    public static boolean checkForHp() {
       return checkForPresence("os.name", "hp");
    }
 
    public static boolean checkForSolaris() {
        return checkForPresence("os.name", "sun");
    }

    public static boolean checkForWindows() {
        return checkForPresence("os.name", "win");
    }

    public static boolean checkForMac() {
        return checkForPresence("os.name", "mac");
    }

    private static boolean checkForPresence(String key, String value) {
        try {
            String tmp=System.getProperty(key);
            return tmp != null && tmp.trim().toLowerCase().startsWith(value);
        }
        catch(Throwable t) {
            return false;
        }
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

    public static <T> Vector<T> unmodifiableVector(Vector<? extends T> v) {
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

    /** IP related utilities */

    public static InetAddress getIPv4Localhost() throws UnknownHostException {
    	return getLocalhost(StackType.IPv4) ;
    }

    public static InetAddress getIPv6Localhost() throws UnknownHostException {
    	return getLocalhost(StackType.IPv6) ;
    }

    public static InetAddress getLocalhost(StackType ip_version) throws UnknownHostException {
    	if (ip_version == StackType.IPv4)
    		return InetAddress.getByName("127.0.0.1") ;
    	else
    		return InetAddress.getByName("::1") ;
    }

 

    /**
     * Returns the first non-loopback address on any interface on the current host.
     *
     * @param ip_version Constraint on IP version of address to be returned, 4 or 6
     */
    public static InetAddress getFirstNonLoopbackAddress(StackType ip_version) throws SocketException {
    	InetAddress address = null ;

    	Enumeration intfs = NetworkInterface.getNetworkInterfaces();
    	while(intfs.hasMoreElements()) {
    		NetworkInterface intf=(NetworkInterface)intfs.nextElement();
            // if(!intf.isUp() || intf.isLoopback())
            if(Util.isDownOrLoopback(intf))
                continue;
    		address = getFirstNonLoopbackAddress(intf, ip_version) ;
    		if (address != null) {
    			return address ;
    		}
    	}
    	return null ;
    }


    public static boolean isDownOrLoopback(NetworkInterface intf) {
        boolean is_up=true, is_loopback=false;

        if(NETWORK_INTERFACE_IS_UP != null) {
            try {
                Boolean retval=(Boolean)NETWORK_INTERFACE_IS_UP.invoke(intf);
                if(retval != null)
                    is_up=retval.booleanValue();
            }
            catch(Throwable t) {
            }
        }


        if(NETWORK_INTERFACE_IS_LOOPBACK != null) {
            try {
                Boolean retval=(Boolean)NETWORK_INTERFACE_IS_LOOPBACK.invoke(intf);
                if(retval != null)
                    is_loopback=retval.booleanValue();
            }
            catch(Throwable t) {
            }
        }

        return !is_up || is_loopback; 
    }


    /**
     * Returns the first non-loopback address on the given interface on the current host.
     *
     * @param intf the interface to be checked
     * @param ip_version Constraint on IP version of address to be returned, 4 or 6
     */    
    public static InetAddress getFirstNonLoopbackAddress(NetworkInterface intf, StackType ip_version) throws SocketException {
    	if (intf == null) 
    		throw new IllegalArgumentException("Network interface pointer is null") ; 

    	for(Enumeration addresses=intf.getInetAddresses(); addresses.hasMoreElements();) {
    		InetAddress address=(InetAddress)addresses.nextElement();
    		if(!address.isLoopbackAddress()) {
    			if ((address instanceof Inet4Address && ip_version == StackType.IPv4) ||
                        (address instanceof Inet6Address && ip_version == StackType.IPv6))
    				return address;
    		}
    	}
    	return null ;
    }

    /**
     * A function to check if an interface supports an IP version (i.e has addresses 
     * defined for that IP version).
     * 
     * @param intf
     * @return
     */
    public static boolean interfaceHasIPAddresses(NetworkInterface intf, StackType ip_version) throws SocketException,UnknownHostException {
        boolean supportsVersion = false ;
        if (intf != null) {
            // get all the InetAddresses defined on the interface
            Enumeration addresses = intf.getInetAddresses() ;
            while (addresses != null && addresses.hasMoreElements()) {
                // get the next InetAddress for the current interface
                InetAddress address = (InetAddress) addresses.nextElement() ;

                // check if we find an address of correct version
                if ((address instanceof Inet4Address && (ip_version == StackType.IPv4)) ||
                        (address instanceof Inet6Address && (ip_version == StackType.IPv6))) {
                    supportsVersion = true ;
                    break ;
                }
            }
        }
        else {
            throw new UnknownHostException("network interface " + intf + " not found") ;
        }
        return supportsVersion ;
    }         
        
    /**
     * Tries to determine the type of IP stack from the available interfaces and their addresses and from the
     * system properties (java.net.preferIPv4Stack and java.net.preferIPv6Addresses)
     * @return StackType.IPv4 for an IPv4 only stack, StackYTypeIPv6 for an IPv6 only stack, and StackType.Unknown
     * if the type cannot be detected
     */
    public static StackType getIpStackType() {
        boolean isIPv4StackAvailable = isStackAvailable(true) ;
    	boolean isIPv6StackAvailable = isStackAvailable(false) ;
    	
		// if only IPv4 stack available
		if (isIPv4StackAvailable && !isIPv6StackAvailable) {
			return StackType.IPv4;
		}
		// if only IPv6 stack available
		else if (isIPv6StackAvailable && !isIPv4StackAvailable) {
			return StackType.IPv6;
		}
		// if dual stack
		else if (isIPv4StackAvailable && isIPv6StackAvailable) {
			// get the System property which records user preference for a stack on a dual stack machine
            if(Boolean.getBoolean(Global.IPv4)) // has preference over java.net.preferIPv6Addresses
                return StackType.IPv4;
            if(Boolean.getBoolean(Global.IPv6))
                return StackType.IPv6;
            return StackType.IPv6;
		}
		return StackType.Unknown;
    }
    


	public static boolean isStackAvailable(boolean ipv4) {
        Collection<InetAddress> all_addrs=getAllAvailableAddresses();
        for(InetAddress addr: all_addrs)
            if(ipv4 && addr instanceof Inet4Address || (!ipv4 && addr instanceof Inet6Address))
                return true;
        return false;
    }
    
	
    public static List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
        List<NetworkInterface> retval=new ArrayList<NetworkInterface>(10);
        NetworkInterface intf;
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            intf=(NetworkInterface)en.nextElement();
            retval.add(intf);
        }
        return retval;
    }

    public static Collection<InetAddress> getAllAvailableAddresses() {
        Set<InetAddress> retval=new HashSet<InetAddress>();
        Enumeration en;

        try {
            en=NetworkInterface.getNetworkInterfaces();
            if(en == null)
                return retval;
            while(en.hasMoreElements()) {
                NetworkInterface intf=(NetworkInterface)en.nextElement();
                Enumeration<InetAddress> addrs=intf.getInetAddresses();
                while(addrs.hasMoreElements())
                    retval.add(addrs.nextElement());
            }
        }
        catch(SocketException e) {
            e.printStackTrace();
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



    public static boolean isCoordinator(JChannel ch) {
        if(ch == null) return false;
        View view=ch.getView();
        if(view == null)
            return false;
        Address local_addr=ch.getAddress();
        if(local_addr == null)
            return false;
        Vector<Address> mbrs=view.getMembers();
        return !(mbrs == null || mbrs.isEmpty()) && local_addr.equals(mbrs.firstElement());
    }


    public static MBeanServer getMBeanServer() {
		ArrayList servers = MBeanServerFactory.findMBeanServer(null);
		if (servers != null && !servers.isEmpty()) {
			// return 'jboss' server if available
			for (int i = 0; i < servers.size(); i++) {
				MBeanServer srv = (MBeanServer) servers.get(i);
				if ("jboss".equalsIgnoreCase(srv.getDefaultDomain()))
					return srv;
			}

			// return first available server
			return (MBeanServer) servers.get(0);
		}
		else {
			//if it all fails, create a default
			return MBeanServerFactory.createMBeanServer();
		}
	}


    public static void registerChannel(JChannel channel, String name) {
        MBeanServer server=Util.getMBeanServer();
        if(server != null) {
            try {
                JmxConfigurator.registerChannel(channel,
                                                server,
                                                (name != null? name : "jgroups"),
                                                channel.getClusterName(),
                                                true);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
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
     * Go through the input string and replace any occurance of ${p} with the
     * props.getProperty(p) value. If there is no such property p defined, then
     * the ${p} reference will remain unchanged.
     *
     * If the property reference is of the form ${p:v} and there is no such
     * property p, then the default value v will be returned.
     *
     * If the property reference is of the form ${p1,p2} or ${p1,p2:v} then the
     * primary and the secondary properties will be tried in turn, before
     * returning either the unchanged input, or the default value.
     *
     * The property ${/} is replaced with System.getProperty("file.separator")
     * value and the property ${:} is replaced with
     * System.getProperty("path.separator").
     *
     * @param string -
     *                the string with possible ${} references
     * @param props -
     *                the source for ${x} property ref values, null means use
     *                System.getProperty()
     * @return the input string with all property references replaced if any. If
     *         there are no valid references the input string will be returned.
     * @throws {@link java.security.AccessControlException}
     *                 when not authorised to retrieved system properties
     */
    public static String replaceProperties(final String string, final Properties props) {
        /** File separator value */
        final String FILE_SEPARATOR=File.separator;

        /** Path separator value */
        final String PATH_SEPARATOR=File.pathSeparator;

        /** File separator alias */
        final String FILE_SEPARATOR_ALIAS="/";

        /** Path separator alias */
        final String PATH_SEPARATOR_ALIAS=":";

        // States used in property parsing
        final int NORMAL=0;
        final int SEEN_DOLLAR=1;
        final int IN_BRACKET=2;
        final char[] chars=string.toCharArray();
        StringBuilder buffer=new StringBuilder();
        boolean properties=false;
        int state=NORMAL;
        int start=0;
        for(int i=0;i < chars.length;++i) {
            char c=chars[i];

            // Dollar sign outside brackets
            if(c == '$' && state != IN_BRACKET)
                state=SEEN_DOLLAR;

            // Open bracket immediatley after dollar
            else if(c == '{' && state == SEEN_DOLLAR) {
                buffer.append(string.substring(start, i - 1));
                state=IN_BRACKET;
                start=i - 1;
            }

            // No open bracket after dollar
            else if(state == SEEN_DOLLAR)
                state=NORMAL;

            // Closed bracket after open bracket
            else if(c == '}' && state == IN_BRACKET) {
                // No content
                if(start + 2 == i) {
                    buffer.append("${}"); // REVIEW: Correct?
                }
                else // Collect the system property
                {
                    String value=null;

                    String key=string.substring(start + 2, i);

                    // check for alias
                    if(FILE_SEPARATOR_ALIAS.equals(key)) {
                        value=FILE_SEPARATOR;
                    }
                    else if(PATH_SEPARATOR_ALIAS.equals(key)) {
                        value=PATH_SEPARATOR;
                    }
                    else {
                        // check from the properties
                        if(props != null)
                            value=props.getProperty(key);
                        else
                            value=System.getProperty(key);

                        if(value == null) {
                            // Check for a default value ${key:default}
                            int colon=key.indexOf(':');
                            if(colon > 0) {
                                String realKey=key.substring(0, colon);
                                if(props != null)
                                    value=props.getProperty(realKey);
                                else
                                    value=System.getProperty(realKey);

                                if(value == null) {
                                    // Check for a composite key, "key1,key2"
                                    value=resolveCompositeKey(realKey, props);

                                    // Not a composite key either, use the specified default
                                    if(value == null)
                                        value=key.substring(colon + 1);
                                }
                            }
                            else {
                                // No default, check for a composite key, "key1,key2"
                                value=resolveCompositeKey(key, props);
                            }
                        }
                    }

                    if(value != null) {
                        properties=true;
                        buffer.append(value);
                    }
                }
                start=i + 1;
                state=NORMAL;
            }
        }

        // No properties
        if(properties == false)
            return string;

        // Collect the trailing characters
        if(start != chars.length)
            buffer.append(string.substring(start, chars.length));

        // Done
        return buffer.toString();
    }

    /**
     * Try to resolve a "key" from the provided properties by checking if it is
     * actually a "key1,key2", in which case try first "key1", then "key2". If
     * all fails, return null.
     *
     * It also accepts "key1," and ",key2".
     *
     * @param key
     *                the key to resolve
     * @param props
     *                the properties to use
     * @return the resolved key or null
     */
    private static String resolveCompositeKey(String key, Properties props) {
        String value=null;

        // Look for the comma
        int comma=key.indexOf(',');
        if(comma > -1) {
            // If we have a first part, try resolve it
            if(comma > 0) {
                // Check the first part
                String key1=key.substring(0, comma);
                if(props != null)
                    value=props.getProperty(key1);
                else
                    value=System.getProperty(key1);
            }
            // Check the second part, if there is one and first lookup failed
            if(value == null && comma < key.length() - 1) {
                String key2=key.substring(comma + 1);
                if(props != null)
                    value=props.getProperty(key2);
                else
                    value=System.getProperty(key2);
            }
        }
        // Return whatever we've found or null
        return value;
    }

//    /**
//     * Replaces variables with values from system properties. If a system property is not found, the property is
//     * removed from the output string
//     * @param input
//     * @return
//     */
//    public static String substituteVariables(String input) throws Exception {
//        Collection<Configurator.ProtocolConfiguration> configs=Configurator.parseConfigurations(input);
//        for(Configurator.ProtocolConfiguration config: configs) {
//            for(Iterator<Map.Entry<String,String>> it=config.getProperties().entrySet().iterator(); it.hasNext();) {
//                Map.Entry<String,String> entry=it.next();
//
//
//            }
//        }
//
//
//        return null;
//    }


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

        while(retval.contains("${")) { // handle multiple variables in val
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

    public static String getProperty(String s) {
        String var, default_val, retval=null;
        int index=s.indexOf(":");
        if(index >= 0) {
            var=s.substring(0, index);
            default_val=s.substring(index+1);
            if(default_val != null && default_val.length() > 0)
                default_val=default_val.trim();
            // retval=System.getProperty(var, default_val);
            retval=_getProperty(var, default_val);
        }
        else {
            var=s;
            // retval=System.getProperty(var);
            retval=_getProperty(var, null);
        }
        return retval;
    }

    /**
     * Parses a var which might be comma delimited, e.g. bla,foo:1000: if 'bla' is set, return its value. Else,
     * if 'foo' is set, return its value, else return "1000"
     * @param var
     * @param default_value
     * @return
     */
    private static String _getProperty(String var, String default_value) {
        if(var == null)
            return null;
        List<String> list=parseCommaDelimitedStrings(var);
        if(list == null || list.isEmpty()) {
            list=new ArrayList<String>(1);
            list.add(var);
        }
        String retval=null;
        for(String prop: list) {
            try {
                retval=System.getProperty(prop);
                if(retval != null)
                    return retval;
            }
            catch(Throwable e) {
            }
        }
        return default_value;
    }


     /**
     * Used to convert a byte array in to a java.lang.String object
     * @param bytes the bytes to be converted
     * @return the String representation
     */
    private static String getString(byte[] bytes) {
         StringBuilder sb=new StringBuilder();
         for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            sb.append(0x00FF & b);
            if (i + 1 < bytes.length) {
                sb.append("-");
            }
        }
        return sb.toString();
    }


    /**
     * Converts a java.lang.String in to a MD5 hashed String
     * @param source the source String
     * @return the MD5 hashed version of the string
     */
    public static String md5(String source) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(source.getBytes());
            return getString(bytes);
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * Converts a java.lang.String in to a SHA hashed String
     * @param source the source String
     * @return the MD5 hashed version of the string
     */
    public static String sha(String source) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA");
            byte[] bytes = md.digest(source.getBytes());
            return getString(bytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static String methodNameToAttributeName(String methodName) {
        methodName=methodName.startsWith("get") || methodName.startsWith("set")? methodName.substring(3): methodName;
        methodName=methodName.startsWith("is")? methodName.substring(2) : methodName;
        // Pattern p=Pattern.compile("[A-Z]+");
        Matcher m=METHOD_NAME_TO_ATTR_NAME_PATTERN.matcher(methodName);
        StringBuffer sb=new StringBuffer();
        while(m.find()) {
            int start=m.start(), end=m.end();
            String str=methodName.substring(start, end).toLowerCase();
            if(str.length() > 1) {
                String tmp1=str.substring(0, str.length() -1);
                String tmp2=str.substring(str.length() -1);
                str=tmp1 + "_" + tmp2;
            }
            if(start == 0) {
                m.appendReplacement(sb, str);
            }
            else
                m.appendReplacement(sb, "_" + str);
        }
        m.appendTail(sb);
        return sb.toString();
    }


    public static String attributeNameToMethodName(String attr_name) {
        if(attr_name.contains("_")) {
            // Pattern p=Pattern.compile("_.");
            Matcher m=ATTR_NAME_TO_METHOD_NAME_PATTERN.matcher(attr_name);
            StringBuffer sb=new StringBuffer();
            while(m.find()) {
                m.appendReplacement(sb, attr_name.substring(m.end() - 1, m.end()).toUpperCase());
            }
            m.appendTail(sb);
            char first=sb.charAt(0);
            if(Character.isLowerCase(first)) {
                sb.setCharAt(0, Character.toUpperCase(first));
            }
            return sb.toString();
        }
        else {
            if(Character.isLowerCase(attr_name.charAt(0))) {
                return attr_name.substring(0, 1).toUpperCase() + attr_name.substring(1);
            }
            else {
                return attr_name;
            }
        }
    }

    /**
     * Runs a task on a separate thread
     * @param task
     * @param factory
     * @param group
     * @param thread_name
     */
    public static void runAsync(Runnable task, ThreadFactory factory, ThreadGroup group, String thread_name) {
        Thread thread=factory.newThread(group, task, thread_name);
        thread.start();
    }

}




