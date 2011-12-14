package org.jgroups.util;

import org.jgroups.*;
import org.jgroups.TimeoutException;
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
import org.jgroups.stack.Protocol;
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
import java.lang.reflect.Modifier;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Collection of various utility routines that can not be assigned to other classes.
 * @author Bela Ban
 */
public class Util {

    private static  NumberFormat f;

    private static Map<Class<? extends Object>,Byte> PRIMITIVE_TYPES=new HashMap<Class<? extends Object>,Byte>(15);
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

    private static short COUNTER=1;

    private static Pattern METHOD_NAME_TO_ATTR_NAME_PATTERN=Pattern.compile("[A-Z]+");
    private static Pattern ATTR_NAME_TO_METHOD_NAME_PATTERN=Pattern.compile("_.");


    protected static int   CCHM_INITIAL_CAPACITY=16;
    protected static float CCHM_LOAD_FACTOR=0.75f;
    protected static int   CCHM_CONCURRENCY_LEVEL=16;

    /** The max size of an address list, e.g. used in View or Digest when toString() is called. Limiting this
     * reduces the amount of log data */
    public static int MAX_LIST_PRINT_SIZE=20;

    /**
     * Global thread group to which all (most!) JGroups threads belong
     */
    private static ThreadGroup GLOBAL_GROUP=new ThreadGroup("JGroups") {
        public void uncaughtException(Thread t, Throwable e) {
            LogFactory.getLog("org.jgroups").error("uncaught exception in " + t + " (thread group=" + GLOBAL_GROUP + " )", e);
            final ThreadGroup tgParent = getParent();
            if(tgParent != null) {
                tgParent.uncaughtException(t,e);
            }
        }
    };

    public static ThreadGroup getGlobalThreadGroup() {
        return GLOBAL_GROUP;
    }

    public static enum AddressScope {GLOBAL, SITE_LOCAL, LINK_LOCAL, LOOPBACK, NON_LOOPBACK};

    private static StackType ip_stack_type=_getIpStackType();


    static {
        /* Trying to get value of resolve_dns. PropertyPermission not granted if
        * running in an untrusted environment  with JNLP */
        try {
            resolve_dns=Boolean.valueOf(System.getProperty("resolve.dns","false"));
        }
        catch (SecurityException ex){
            resolve_dns=false;
        }
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        // f.setMinimumFractionDigits(2);
        f.setMaximumFractionDigits(2);

        PRIMITIVE_TYPES.put(Boolean.class, TYPE_BOOLEAN);
        PRIMITIVE_TYPES.put(Byte.class, TYPE_BYTE);
        PRIMITIVE_TYPES.put(Character.class, TYPE_CHAR);
        PRIMITIVE_TYPES.put(Double.class, TYPE_DOUBLE);
        PRIMITIVE_TYPES.put(Float.class, TYPE_FLOAT);
        PRIMITIVE_TYPES.put(Integer.class, TYPE_INT);
        PRIMITIVE_TYPES.put(Long.class, TYPE_LONG);
        PRIMITIVE_TYPES.put(Short.class, TYPE_SHORT);
        PRIMITIVE_TYPES.put(String.class, TYPE_STRING);
        PRIMITIVE_TYPES.put(byte[].class, TYPE_BYTEARRAY);

        if(ip_stack_type == StackType.Unknown)
            ip_stack_type=StackType.IPv6;

        try {
            String cchm_initial_capacity=System.getProperty(Global.CCHM_INITIAL_CAPACITY);
            if(cchm_initial_capacity != null)
                CCHM_INITIAL_CAPACITY=Integer.valueOf(cchm_initial_capacity);
        } catch(SecurityException ex) {}

        try {
            String cchm_load_factor=System.getProperty(Global.CCHM_LOAD_FACTOR);
            if(cchm_load_factor != null)
                CCHM_LOAD_FACTOR=Float.valueOf(cchm_load_factor);
        } catch(SecurityException ex) {}

        try {
            String cchm_concurrency_level=System.getProperty(Global.CCHM_CONCURRENCY_LEVEL);
            if(cchm_concurrency_level != null)
                CCHM_CONCURRENCY_LEVEL=Integer.valueOf(cchm_concurrency_level);
        } catch(SecurityException ex) {}

        try {
            String tmp=System.getProperty(Global.MAX_LIST_PRINT_SIZE);
            if(tmp != null)
                MAX_LIST_PRINT_SIZE=Integer.valueOf(tmp);
        } catch(SecurityException ex) {
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
    public static void waitUntilAllChannelsHaveSameSize(long timeout, long interval, Channel... channels) throws TimeoutException {
        int size=channels.length;

        if(interval >= timeout || timeout <= 0)
            throw new IllegalArgumentException("interval needs to be smaller than timeout or timeout needs to be > 0");
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_channels_have_correct_size=true;
            for(Channel ch: channels) {
                View view=ch.getView();
                if(view == null || view.size() != size) {
                    all_channels_have_correct_size=false;
                    break;
                }
            }
            if(all_channels_have_correct_size)
                return;
            Util.sleep(interval);
        }
        View[] views=new View[channels.length];
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < channels.length; i++) {
            views[i]=channels[i].getView();
            sb.append(channels[i].getName()).append(": ").append(views[i]).append("\n");
        }
        for(View view: views)
            if(view == null || view.size() != size)
                throw new TimeoutException("Timeout " + timeout + " kicked in, views are:\n" + sb);
    }


    public static void addFlush(Channel ch, FLUSH flush) {
        if(ch == null || flush == null)
            throw new IllegalArgumentException("ch and flush have to be non-null");
        ProtocolStack stack=ch.getProtocolStack();
        stack.insertProtocolAtTop(flush);
    }


    public static void setScope(Message msg, short scope) {
        SCOPE.ScopeHeader hdr=SCOPE.ScopeHeader.createMessageHeader(scope);
        msg.putHeader(Global.SCOPE_ID, hdr);
        msg.setFlag(Message.SCOPED);
    }

    public static short getScope(Message msg) {
        SCOPE.ScopeHeader hdr=(SCOPE.ScopeHeader)msg.getHeader(Global.SCOPE_ID);
        return hdr != null? hdr.getScope() : 0;
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
        return objectFromByteBuffer(buffer, 0, buffer.length);
    }


    public static Object objectFromByteBuffer(byte[] buffer, int offset, int length) throws Exception {
        if(buffer == null) return null;
        Object retval=null;
        byte type=buffer[offset];


        switch(type) {
            case TYPE_NULL:
                return null;
            case TYPE_STREAMABLE:
                ByteArrayInputStream in_stream=new ExposedByteArrayInputStream(buffer, offset+1, length-1);
                InputStream in=new DataInputStream(in_stream);
                retval=readGenericStreamable((DataInputStream)in);
                break;
            case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                in_stream=new ExposedByteArrayInputStream(buffer, offset+1, length-1);
                in=new ObjectInputStream(in_stream); // changed Nov 29 2004 (bela)
                try {
                    retval=((ObjectInputStream)in).readObject();
                }
                finally {
                    Util.close(in);
                }
                break;
            case TYPE_BOOLEAN:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).get() == 1;
            case TYPE_BYTE:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).get();
            case TYPE_CHAR:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).getChar();
            case TYPE_DOUBLE:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).getDouble();
            case TYPE_FLOAT:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).getFloat();
            case TYPE_INT:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).getInt();
            case TYPE_LONG:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).getLong();
            case TYPE_SHORT:
                return ByteBuffer.wrap(buffer, offset + 1, length - 1).getShort();
            case TYPE_STRING:
                byte[] tmp=new byte[length -1];
                System.arraycopy(buffer, offset +1, tmp, 0, length -1);
                return new String(tmp);
            case TYPE_BYTEARRAY:
                tmp=new byte[length -1];
                System.arraycopy(buffer, offset +1, tmp, 0, length -1);
                return tmp;
            default:
                throw new IllegalArgumentException("type " + type + " is invalid");
        }
        return retval;
    }




    /**
     * Serializes/Streams an object into a byte buffer.
     * The object has to implement interface Serializable or Externalizable or Streamable. 
     */
    public static byte[] objectToByteBuffer(Object obj) throws Exception {
        if(obj == null)
            return ByteBuffer.allocate(Global.BYTE_SIZE).put(TYPE_NULL).array();

        if(obj instanceof Streamable) {
            final ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(128);
            final ExposedDataOutputStream out=new ExposedDataOutputStream(out_stream);
            out_stream.write(TYPE_STREAMABLE);
            writeGenericStreamable((Streamable)obj, out);
            return out_stream.toByteArray();
        }

        Byte type=PRIMITIVE_TYPES.get(obj.getClass());
        if(type == null) { // will throw an exception if object is not serializable
            final ExposedByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(128);
            out_stream.write(TYPE_SERIALIZABLE);
            ObjectOutputStream out=new ObjectOutputStream(out_stream);
            out.writeObject(obj);
            out.close();
            return out_stream.toByteArray();
        }

        switch(type) {
            case TYPE_BOOLEAN:
                return ByteBuffer.allocate(Global.BYTE_SIZE * 2).put(TYPE_BOOLEAN)
                  .put((Boolean)obj? (byte)1 : (byte)0).array();
            case TYPE_BYTE:
                return ByteBuffer.allocate(Global.BYTE_SIZE *2).put(TYPE_BYTE).put((Byte)obj).array();
            case TYPE_CHAR:
                return ByteBuffer.allocate(Global.BYTE_SIZE *3).put(TYPE_CHAR).putChar((Character)obj).array();
            case TYPE_DOUBLE:
                return ByteBuffer.allocate(Global.BYTE_SIZE + Global.DOUBLE_SIZE).put(TYPE_DOUBLE)
                        .putDouble((Double)obj).array();
            case TYPE_FLOAT:
                return ByteBuffer.allocate(Global.BYTE_SIZE + Global.FLOAT_SIZE).put(TYPE_FLOAT)
                        .putFloat((Float)obj).array();
            case TYPE_INT:
                return ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE).put(TYPE_INT)
                        .putInt((Integer)obj).array();
            case TYPE_LONG:
                return ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE).put(TYPE_LONG)
                        .putLong((Long)obj).array();
            case TYPE_SHORT:
                return ByteBuffer.allocate(Global.BYTE_SIZE + Global.SHORT_SIZE).put(TYPE_SHORT)
                        .putShort((Short)obj).array();
            case TYPE_STRING:
                String str=(String)obj;
                byte[] buf=new byte[str.length()];
                for(int i=0; i < buf.length; i++)
                    buf[i]=(byte)str.charAt(i);
                return ByteBuffer.allocate(Global.BYTE_SIZE + buf.length).put(TYPE_STRING).put(buf, 0, buf.length).array();
            case TYPE_BYTEARRAY:
                buf=(byte[])obj;
                return ByteBuffer.allocate(Global.BYTE_SIZE + buf.length).put(TYPE_BYTEARRAY)
                        .put(buf, 0, buf.length).array();
            default:
                throw new IllegalArgumentException("type " + type + " is invalid");
        }

    }



    public static void objectToStream(Object obj, DataOutput out) throws Exception {
        if(obj == null) {
            out.write(TYPE_NULL);
            return;
        }

        Byte type;
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
                        ObjectOutputStream oos=new ObjectOutputStream((OutputStream)out);
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
            ObjectOutputStream tmp=new ObjectOutputStream((OutputStream)out);
            tmp.writeObject(obj);
        }
    }



    public static Object objectFromStream(DataInput in) throws Exception {
        if(in == null) return null;
        Object retval=null;
        byte b=in.readByte();

        switch(b) {
            case TYPE_NULL:
                return null;
            case TYPE_STREAMABLE:
                retval=readGenericStreamable(in);
                break;
            case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                ObjectInputStream tmp=new ObjectInputStream((InputStream)in);
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
                    ObjectInputStream ois=new ObjectInputStream((InputStream)in);
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




    public static Streamable streamableFromByteBuffer(Class<? extends Streamable> cl, byte[] buffer) throws Exception {
        if(buffer == null) return null;
        Streamable retval=null;
        ByteArrayInputStream in_stream=new ExposedByteArrayInputStream(buffer);
        DataInputStream in=new DataInputStream(in_stream); // changed Nov 29 2004 (bela)
        retval=cl.newInstance();
        retval.readFrom(in);
        in.close();
        return retval;
    }


    public static Streamable streamableFromByteBuffer(Class<? extends Streamable> cl, byte[] buffer, int offset, int length) throws Exception {
        if(buffer == null) return null;
        Streamable retval=null;
        ByteArrayInputStream in_stream=new ExposedByteArrayInputStream(buffer, offset, length);
        DataInputStream in=new DataInputStream(in_stream); // changed Nov 29 2004 (bela)
        retval=cl.newInstance();
        retval.readFrom(in);
        in.close();
        return retval;
    }

    public static byte[] streamableToByteBuffer(Streamable obj) throws Exception {
        byte[] result=null;
        final ByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new ExposedDataOutputStream(out_stream);
        obj.writeTo(out);
        result=out_stream.toByteArray();
        out.close();
        return result;
    }


    public static byte[] collectionToByteBuffer(Collection<Address> c) throws Exception {
        byte[] result=null;
        final ByteArrayOutputStream out_stream=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new ExposedDataOutputStream(out_stream);
        Util.writeAddresses(c, out);
        result=out_stream.toByteArray();
        out.close();
        return result;
    }



    public static void writeAuthToken(AuthToken token, DataOutput out) throws Exception{
        Util.writeString(token.getName(), out);
        token.writeTo(out);
    }

    public static AuthToken readAuthToken(DataInput in) throws Exception {
        try{
            String type = Util.readString(in);
            Object obj = Class.forName(type).newInstance();
            AuthToken token = (AuthToken) obj;
            token.readFrom(in);
            return token;
        }
        catch(ClassNotFoundException cnfe) {
            return null;
        }
    }


    public static void writeView(View view, DataOutput out) throws Exception {
        if(view == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeBoolean(view instanceof MergeView);
        view.writeTo(out);
    }


    public static View readView(DataInput in) throws Exception {
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

    public static void writeViewId(ViewId vid, DataOutput out) throws Exception {
        if(vid == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        vid.writeTo(out);
    }

    public static ViewId readViewId(DataInput in) throws Exception {
        if(in.readBoolean() == false)
            return null;
        ViewId retval=new ViewId();
        retval.readFrom(in);
        return retval;
    }


    public static void writeAddress(Address addr, DataOutput out) throws Exception {
        byte flags=0;
        boolean streamable_addr=true;

        if(addr == null) {
            flags=Util.setFlag(flags, Address.NULL);
            out.writeByte(flags);
            return;
        }

        Class<? extends Address> clazz=addr.getClass();
        if(clazz.equals(UUID.class)) {
            flags=Util.setFlag(flags, Address.UUID_ADDR);
        }
        else if(clazz.equals(IpAddress.class)) {
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

    public static Address readAddress(DataInput in) throws Exception {
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

    public static int size(ViewId vid) {
        int retval=Global.BYTE_SIZE; // presence
        if(vid != null)
            retval+=vid.serializedSize();
        return retval;
    }

    public static int size(String s) {
        int retval=Global.BYTE_SIZE;
        if(s != null)
            retval+=s.length() +2;
        return retval;
    }

    public static int size(byte[] buf) {
        int retval=Global.BYTE_SIZE + Global.INT_SIZE;
        if(buf != null)
            retval+=buf.length;
        return retval;
    }

    private static Address readOtherAddress(DataInput in) throws Exception {
        short magic_number=in.readShort();
        Class<Address> cl=ClassConfigurator.get(magic_number);
        if(cl == null)
            throw new RuntimeException("class for magic number " + magic_number + " not found");
        Address addr=cl.newInstance();
        addr.readFrom(in);
        return addr;
    }

    private static void writeOtherAddress(Address addr, DataOutput out) throws Exception {
        short magic_number=ClassConfigurator.getMagicNumber(addr.getClass());

        // write the class info
        if(magic_number == -1)
            throw new RuntimeException("magic number " + magic_number + " not found");

        out.writeShort(magic_number);
        addr.writeTo(out);
    }

    /**
     * Writes a Vector of Addresses. Can contain 65K addresses at most
     *
     * @param v A Collection<Address>
     * @param out
     * @throws Exception
     */
    public static void writeAddresses(Collection<? extends Address> v, DataOutput out) throws Exception {
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
     *
     * @param in
     * @param cl The type of Collection, e.g. ArrayList.class
     * @return Collection of Address objects
     * @throws Exception
     */
    public static Collection<? extends Address> readAddresses(DataInput in, Class cl) throws Exception {
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




    public static void writeStreamable(Streamable obj, DataOutput out) throws Exception {
        if(obj == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        obj.writeTo(out);
    }


    public static Streamable readStreamable(Class clazz, DataInput in) throws Exception {
        Streamable retval=null;
        if(in.readBoolean() == false)
            return null;
        retval=(Streamable)clazz.newInstance();
        retval.readFrom(in);
        return retval;
    }


    public static void writeGenericStreamable(Streamable obj, DataOutput out) throws Exception {
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



    public static Streamable readGenericStreamable(DataInput in) throws Exception {
        Streamable retval=null;
        int b=in.readByte();
        if(b == 0)
            return null;

        boolean use_magic_number=in.readBoolean();
        String classname;
        Class clazz;

        if(use_magic_number) {
            short magic_number=in.readShort();
            clazz=ClassConfigurator.get(magic_number);
            if (clazz==null)
                throw new ClassNotFoundException("Class for magic number "+magic_number+" cannot be found.");
        }
        else {
            classname=in.readUTF();
            clazz=ClassConfigurator.get(classname);
            if (clazz==null)
                throw new ClassNotFoundException(classname);
        }

        retval=(Streamable)clazz.newInstance();
        retval.readFrom(in);
        return retval;
    }

    public static void writeClass(Class<?> classObject, DataOutput out) throws Exception {
        short magic_number=ClassConfigurator.getMagicNumber(classObject);
        // write the magic number or the class name
        if(magic_number == -1) {
            out.writeBoolean(false);
            out.writeUTF(classObject.getName());
        }
        else {
            out.writeBoolean(true);
            out.writeShort(magic_number);
        }
    }

    public static Class<?> readClass(DataInput in) throws Exception {
        Class<?> clazz;
        boolean use_magic_number = in.readBoolean();
        if(use_magic_number) {
            short magic_number=in.readShort();
            clazz=ClassConfigurator.get(magic_number);
            if(clazz == null)
                throw new ClassNotFoundException("Class for magic number "+magic_number+" cannot be found.");
        }
        else {
            String classname=in.readUTF();
            clazz=ClassConfigurator.get(classname);
            if(clazz == null)
                throw new ClassNotFoundException(classname);
        }

        return clazz;
    }

    public static void writeObject(Object obj, DataOutput out) throws Exception {
        if(obj instanceof Streamable) {
            out.writeInt(-1);
            writeGenericStreamable((Streamable)obj, out);
        }
        else {
            byte[] buf=objectToByteBuffer(obj);
            out.writeInt(buf.length);
            out.write(buf, 0, buf.length);
        }
    }

    public static Object readObject(DataInput in) throws Exception {
        int len=in.readInt();
        if(len == -1)
            return readGenericStreamable(in);
        
        byte[] buf=new byte[len];
        in.readFully(buf, 0, len);
        return objectFromByteBuffer(buf);
    }



    public static void writeString(String s, DataOutput out) throws Exception {
        if(s != null) {
            out.write(1);
            out.writeUTF(s);
        }
        else {
            out.write(0);
        }
    }


    public static String readString(DataInput in) throws Exception {
        int b=in.readByte();
        if(b == 1)
            return in.readUTF();
        return null;
    }


    public static String readFile(String filename) throws FileNotFoundException {
        FileInputStream in=new FileInputStream(filename);
        return readContents(in);
    }


    public static String readContents(InputStream input) {
        StringBuilder sb=new StringBuilder();
        int ch;
        while(true) {
            try {
                ch=input.read();
                if(ch != -1)
                    sb.append((char)ch);
                else
                    break;
            }
            catch(IOException e) {
                break;
            }
        }
        return sb.toString();
    }

    public static byte[] readFileContents(String filename) throws IOException {
        File file=new File(filename);
        if(!file.exists())
            throw new FileNotFoundException(filename);
        long length=file.length();
        byte contents[]=new byte[(int)length];
        InputStream in=new BufferedInputStream(new FileInputStream(filename));
        int bytes_read=0;

        for(;;) {
            int tmp=in.read(contents, bytes_read, (int)(length - bytes_read));
            if(tmp == -1)
                break;
            bytes_read+=tmp;
            if(bytes_read == length)
                break;
        }
        return contents;
    }

    public static byte[] readFileContents(InputStream input) throws IOException {
        byte contents[]=new byte[10000], buf[]=new byte[1024];
        InputStream in=new BufferedInputStream(input);
        int bytes_read=0;

        for(;;) {
            int tmp=in.read(buf, 0, buf.length);
            if(tmp == -1)
                break;
            System.arraycopy(buf, 0, contents, bytes_read, tmp);
            bytes_read+=tmp;
        }

        byte[] retval=new byte[bytes_read];
        System.arraycopy(contents, 0, retval, 0, bytes_read);
        return retval;
    }


    public static String parseString(DataInput in) {
        StringBuilder sb=new StringBuilder();
        int ch;

        // read white space
        while(true) {
            try {
                ch=in.readByte();
                if(ch == -1) {
                    return null; // eof
                }
                if(Character.isWhitespace(ch)) {
                    if(ch == '\n')
                        return null;
                }
                else {
                    sb.append((char)ch);
                    break;
                }
            }
            catch(EOFException eof) {
                return null;
            }
            catch(IOException e) {
                break;
            }
        }

        while(true) {
            try {
                ch=in.readByte();
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

    public static double readDoubleFromStdin(String message) throws Exception {
        String tmp=readStringFromStdin(message);
        return Double.parseDouble(tmp);
    }

    public static int readIntFromStdin(String message) throws Exception {
        String tmp=readStringFromStdin(message);
        return Integer.parseInt(tmp);
    }


    public static void writeByteBuffer(byte[] buf, DataOutput out) throws Exception {
        writeByteBuffer(buf, 0, buf.length, out);
    }

     public static void writeByteBuffer(byte[] buf, int offset, int length, DataOutput out) throws Exception {
        if(buf != null) {
            out.write(1);
            out.writeInt(length);
            out.write(buf, offset, length);
        }
        else {
            out.write(0);
        }
    }

    public static byte[] readByteBuffer(DataInput in) throws Exception {
        int b=in.readByte();
        if(b == 1) {
            b=in.readInt();
            byte[] buf=new byte[b];
            in.readFully(buf, 0, buf.length);
            return buf;
        }
        return null;
    }


    public static Buffer messageToByteBuffer(Message msg) throws Exception {
        ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new ExposedDataOutputStream(output);

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
        ByteArrayInputStream input=new ExposedByteArrayInputStream(buffer, offset, length);
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
       * @throws Exception
       */
    public static Buffer msgListToByteBuffer(List<Message> xmit_list) throws Exception {
        ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new ExposedDataOutputStream(output);
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

        if(a1.hashCode() == a2.hashCode()) // identity
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



    public static String dumpThreads() {
        StringBuilder sb=new StringBuilder();
        ThreadMXBean bean=ManagementFactory.getThreadMXBean();
        long[] ids=bean.getAllThreadIds();
        _printThreads(bean, ids, sb);
        long[] deadlocks=bean.findDeadlockedThreads();
        if(deadlocks != null && deadlocks.length > 0) {
            sb.append("deadlocked threads:\n");
            _printThreads(bean, deadlocks, sb);
        }

        deadlocks=bean.findMonitorDeadlockedThreads();
        if(deadlocks != null && deadlocks.length > 0) {
            sb.append("monitor deadlocked threads:\n");
            _printThreads(bean, deadlocks, sb);
        }
        return sb.toString();
    }


    protected static void _printThreads(ThreadMXBean bean, long[] ids, StringBuilder sb) {
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
                        Map<Short,Header> headers=new HashMap<Short,Header>(m.getHeaders());
                        for(Map.Entry<Short,Header> entry: headers.entrySet()) {
                            short id=entry.getKey();
                            Header value=entry.getValue();
                            String headerToString=null;
                            if(value instanceof FD.FdHeader) {
                                headerToString=value.toString();
                            }
                            else
                                if(value instanceof PingHeader) {
                                    headerToString=ClassConfigurator.getProtocol(id) + "-";
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
                                    headerToString=ClassConfigurator.getProtocol(id) + "-" + (value == null ? "null" : value.toString());
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


    /**
     * MByte nowadays doesn't mean 1024 * 1024 bytes, but 1 million bytes, see http://en.wikipedia.org/wiki/Megabyte
     * @param bytes
     * @return
     */
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


    public static String printTime(long time, TimeUnit unit) {
        long ns=TimeUnit.NANOSECONDS.convert(time, unit);
        long us=TimeUnit.MICROSECONDS.convert(time, unit);
        long ms=TimeUnit.MILLISECONDS.convert(time, unit);
        long secs=TimeUnit.SECONDS.convert(time, unit);

        if(secs > 0) return secs + "s";
        if(ms > 0) return ms + "ms";
        if(us > 0) return us + " us";
        return ns + "ns";
    }


    public static String format(double value) {
        return f.format(value);
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


   /* public static String[] commands(String path, String separator) {
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


    public static byte[] encode(long num) {
        if(num == 0)
            return new byte[]{0};

        byte bytes_needed=numberOfBytesRequiredForLong(num);
        byte[] buf=new byte[bytes_needed + 1];
        buf[0]=bytes_needed;

        int index=1;
        for(int i=0; i < bytes_needed; i++)
            buf[index++]=getByteAt(num, i);
        return buf;
    }

    static protected byte getByteAt(long num, int index) {
        return (byte)((num >> (index * 8)));
    }

    public static long decode(byte[] buf) {
        if(buf[0] == 0)
            return 0;

        byte length=buf[0];
        return makeLong(buf, 1, length);
    }


    public static void writeLong(long num, DataOutput out) throws Exception {
        byte[] buf=encode(num);
        out.write(buf, 0, buf.length);
    }

    public static long readLong(DataInput in) throws Exception {
        byte len=in.readByte();
        if(len == 0)
            return 0;
        byte[] buf=new byte[len];
        in.readFully(buf, 0, len);
        return makeLong(buf, 0, len);
    }


    static long makeLong(byte[] buf, int offset, int len) {
        long retval=0;
        for(int i=0; i < len; i++) {
            byte b=buf[offset + i];
            retval |= ((long)b & 0xff) << (i * 8);
        }
        return retval;
    }


    /**
     * Encode the highest delivered and received seqnos. The assumption is that the latter is always >= the former, and
     * highest received is not much higher than highest delivered.
     * @param highest_delivered
     * @param highest_received
     * @return
     */
    public static byte[] encodeLongSequence(long highest_delivered, long highest_received) {
        if(highest_received < highest_delivered)
            throw new IllegalArgumentException("highest_received (" + highest_received +
                                                 ") has to be >= highest_delivered (" + highest_delivered + ")");

        if(highest_delivered == 0 &&highest_received == 0)
            return new byte[]{0};

        long delta=highest_received - highest_delivered;

        // encode highest_delivered followed by delta
        byte num_bytes_for_hd=numberOfBytesRequiredForLong(highest_delivered),
          num_bytes_for_delta=numberOfBytesRequiredForLong(delta);

        byte[] buf=new byte[num_bytes_for_hd + num_bytes_for_delta + 1];

        buf[0]=encodeLength(num_bytes_for_hd, num_bytes_for_delta);

        int index=1;
        for(int i=0; i < num_bytes_for_hd; i++)
            buf[index++]=getByteAt(highest_delivered, i);

        for(int i=0; i < num_bytes_for_delta; i++)
            buf[index++]=getByteAt(delta, i);

        return buf;
    }

    private static byte numberOfBytesRequiredForLong(long number) {
        if(number >> 56 != 0) return 8;
        if(number >> 48 != 0) return 7;
        if(number >> 40 != 0) return 6;
        if(number >> 32 != 0) return 5;
        if(number >> 24 != 0) return 4;
        if(number >> 16 != 0) return 3;
        if(number >>  8 != 0) return 2;
        if(number != 0) return 1;
        return 1;
    }

    public static byte size(long number) {
        return (byte)(number == 0? 1 : numberOfBytesRequiredForLong(number) +1);
    }

    /**
     * Writes 2 longs, where the second long needs to be >= the first (we only write the delta !)
     * @param hd
     * @param hr
     * @return
     */
    public static byte size(long hd, long hr) {
        if(hd == 0 && hr == 0)
            return 1;

        byte num_bytes_for_hd=numberOfBytesRequiredForLong(hd),
          num_bytes_for_delta=numberOfBytesRequiredForLong(hr - hd);

        return (byte)(num_bytes_for_hd + num_bytes_for_delta + 1);
    }

    public static long[] decodeLongSequence(byte[] buf) {
        if(buf[0] == 0)
            return new long[]{0,0};

        byte[] lengths=decodeLength(buf[0]);
        long[] seqnos=new long[2];
        seqnos[0]=makeLong(buf, 1, lengths[0]);
        seqnos[1]=makeLong(buf, 1 + lengths[0], lengths[1]) + seqnos[0];

        return seqnos;
    }


    public static void writeLongSequence(long highest_delivered, long highest_received, DataOutput out) throws Exception {
        byte[] buf=encodeLongSequence(highest_delivered, highest_received);
        out.write(buf, 0, buf.length);
    }


    public static long[] readLongSequence(DataInput in) throws Exception {
        byte len=in.readByte();
        if(len == 0)
            return new long[]{0,0};

        byte[] lengths=decodeLength(len);
        long[] seqnos=new long[2];
        byte[] buf=new byte[lengths[0] + lengths[1]];
        in.readFully(buf, 0, buf.length);
        seqnos[0]=makeLong(buf, 0, lengths[0]);
        seqnos[1]=makeLong(buf, lengths[0], lengths[1]) + seqnos[0];
        return seqnos;
    }


    /**
     * Encodes the number of bytes needed into a single byte. The first number is encoded in the first nibble (the
     * first 4 bits), the second number in the second nibble
     * @param len1 The number of bytes needed to store a long. Must be between 0 and 8
     * @param len2 The number of bytes needed to store a long. Must be between 0 and 8
     * @return The byte storing the 2 numbers len1 and len2
     */
    public static byte encodeLength(byte len1, byte len2) {
        byte retval=len2;
        retval |= (len1 << 4);
        return retval;
    }

    public static byte[] decodeLength(byte len) {
        byte[] retval={(byte)0,(byte)0};
        retval[0]=(byte)((len & 0xff) >> 4);
        retval[1]=(byte)(len & ~0xf0); // 0xff is the first nibble set (11110000)
        // retval[1]=(byte)(len << 4);
        // retval[1]=(byte)((retval[1] & 0xff) >> 4);
        return retval;
    }


    public static <T> String printListWithDelimiter(Collection<T> list, String delimiter) {
        return printListWithDelimiter(list, delimiter, MAX_LIST_PRINT_SIZE);
    }


    public static <T> String printListWithDelimiter(Collection<T> list, String delimiter, int limit) {
        boolean first=true;
        StringBuilder sb=new StringBuilder();
        int count=0, size=list.size();
        for(T el: list) {
            if(first) {
                first=false;
            }
            else {
                sb.append(delimiter);
            }
            sb.append(el);
            if(limit > 0 && ++count >= limit) {
                if(size > count)
                    sb.append(" ...");
                break;
            }
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

    public static <T> List<T> newElements(List<T> old_list, List<T> new_list) {
        if(new_list == null)
            return new ArrayList<T>();
        List<T> retval=new ArrayList<T>(new_list);
        if(old_list != null)
            retval.removeAll(old_list);
        return retval;
    }


    /**
     * Selects a random subset of members according to subset_percentage and returns them.
     * Picks no member twice from the same membership. If the percentage is smaller than 1 -> picks 1 member.
     */
    public static List<Address> pickSubset(List<Address> members, double subset_percentage) {
        List<Address> ret=new ArrayList<Address>(), tmp_mbrs;
        int num_mbrs=members.size(), subset_size, index;

        if(num_mbrs == 0) return ret;
        subset_size=(int)Math.ceil(num_mbrs * subset_percentage);

        tmp_mbrs=new ArrayList<Address>(members);

        for(int i=subset_size; i > 0 && !tmp_mbrs.isEmpty(); i--) {
            index=(int)((Math.random() * num_mbrs) % tmp_mbrs.size());
            ret.add(tmp_mbrs.get(index));
            tmp_mbrs.remove(index);
        }

        return ret;
    }



    public static boolean containsViewId(Collection<View> views, ViewId vid) {
        for(View view: views) {
            ViewId tmp=view.getVid();
            if(tmp.equals(vid))
                return true;
        }
        return false;
    }

    public static List<View> detectDifferentViews(Map<Address,View> map) {
        final List<View> ret=new ArrayList<View>();
        for(View view: map.values()) {
            if(view == null)
                continue;
            ViewId vid=view.getVid();
            if(!Util.containsViewId(ret, vid))
                ret.add(view);
        }
        return ret;
    }


    /**
     * Determines the members which take part in a merge. The resulting list consists of all merge coordinators
     * plus members outside a merge partition, e.g. for views A={B,A,C}, B={B,C} and C={B,C}, the merge coordinator
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

    /**
     * Returns the rank of a member in a given view
     * @param view The view
     * @param addr The address of a member
     * @return A value between 1 and view.size(). The first member has rank 1, the second 2 and so on. If the
     * member is not found, 0 is returned
     */
    public static int getRank(View view, Address addr) {
        if(view == null || addr == null)
            return 0;
        List<Address> members=view.getMembers();
        for(int i=0; i < members.size(); i++) {
            Address mbr=members.get(i);
            if(mbr.equals(addr))
                return i+1;
        }
        return 0;
    }

    public static int getRank(Collection<Address> members, Address addr) {
        if(members == null || addr == null)
            return -1;
        int index=0;
        for(Iterator<Address> it=members.iterator(); it.hasNext();) {
            Address mbr=it.next();
            if(mbr.equals(addr))
                return index+1;
            index++;
        }
        return -1;
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

    /**
     * Returns the object next to element in list
     * @param list
     * @param obj
     * @param <T>
     * @return
     */
    public static <T> T pickNext(List<T> list, T obj) {
        if(list == null || obj == null)
            return null;
        Object[] array=list.toArray();
        for(int i=0; i < array.length; i++) {
            T tmp=(T)array[i];
            if(tmp != null && tmp.equals(obj))
                return (T)array[(i+1) % array.length];
        }
        return null;
    }

    /** Returns the next min(N,list.size()) elements after obj */ 
    public static <T> List<T> pickNext(List<T> list, T obj, int num) {
        List<T> retval=new ArrayList<T>();
        if(list == null || list.size() < 2)
            return retval;
        int index=list.indexOf(obj);
        if(index != -1) {
            for(int i=1; i <= num && i < list.size(); i++) {
                T tmp=list.get((index +i) % list.size());
                if(!retval.contains(tmp))
                    retval.add(tmp);
            }
        }
        return retval;
    }


    public static View createView(Address coord, long id, Address ... members) {
        List<Address> mbrs=new ArrayList<Address>();
        mbrs.addAll(Arrays.asList(members));
        return new View(coord, id, mbrs);
    }

    public static JChannel createChannel(Protocol... prots) throws Exception {
        JChannel ch=new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        ch.setProtocolStack(stack);
        for(Protocol prot: prots) {
            stack.addProtocol(prot);
            prot.setProtocolStack(stack);
        }
        stack.init();
        return ch;
    }


    public static Address createRandomAddress() {
        return createRandomAddress(generateLocalName());
    }

    public static Address createRandomAddress(String name) {
        UUID retval=UUID.randomUUID();
        UUID.add(retval, name);
        return retval;
    }

    public static Object[][] createTimer() {
        return new Object[][] {
                {new DefaultTimeScheduler(5)},
                {new TimeScheduler2()},
                {new HashedTimingWheel(5)}
        };
    }

    /**
     * Returns all members that left between 2 views. All members that are element of old_mbrs but not element of
     * new_mbrs are returned.
     */
    public static List<Address> determineLeftMembers(List<Address> old_mbrs, List<Address> new_mbrs) {
        List<Address> retval=new ArrayList<Address>();
        if(old_mbrs == null || new_mbrs == null)
            return retval;

        for(int i=0; i < old_mbrs.size(); i++) {
            Address mbr=old_mbrs.get(i);
            if(!new_mbrs.contains(mbr))
                retval.add(mbr);
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
     will send the peer from FIN to RST state, and the second will cause a signal (Exception).
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
     will send the peer from FIN to RST state, and the second will cause a signal (Exception).
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
    public static void writeFully(ByteBuffer buf, WritableByteChannel out) throws Exception {
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
        try {
            ByteArrayOutputStream output=new ExposedByteArrayOutputStream();
            DataOutputStream out=new ExposedDataOutputStream(output);
            inst.writeTo(out);
            out.flush();
            byte[] data=output.toByteArray();
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

    public static void setField(Field field, Object target, Object value) {
           if(!Modifier.isPublic(field.getModifiers())) {
               field.setAccessible(true);
           }
           try {
               field.set(target, value);
           }
           catch(IllegalAccessException iae) {
               throw new IllegalArgumentException("Could not set field " + field, iae);
           }
       }

       public static Object getField(Field field, Object target) {
           if(!Modifier.isPublic(field.getModifiers())) {
               field.setAccessible(true);
           }
           try {
               return field.get(target);
           }
           catch(IllegalAccessException iae) {
               throw new IllegalArgumentException("Could not get field " + field, iae);
           }
       }


    public static <T> Set<Class<T>> findClassesAssignableFrom(String packageName, Class<T> assignableFrom)
      throws IOException, ClassNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Set<Class<T>> classes = new HashSet<Class<T>>();
        String path = packageName.replace('.', '/');
        URL resource = loader.getResource(path);
        if (resource != null) {
            String filePath = resource.getFile();
            if (filePath != null && new File(filePath).isDirectory()) {
                for (String file : new File(filePath).list()) {
                    if (file.endsWith(".class")) {
                        String name = packageName + '.' + file.substring(0, file.indexOf(".class"));
                        Class<T> clazz =(Class<T>)Class.forName(name);
                        if (assignableFrom.isAssignableFrom(clazz))
                            classes.add(clazz);
                    }
                }
            }
        }
        return classes;
    }

    public static List<Class<?>> findClassesAnnotatedWith(String packageName, Class<? extends Annotation> a) throws IOException, ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        recurse(classes, packageName, a);
        return classes;
    }

    private static void recurse(List<Class<?>> classes, String packageName, Class<? extends Annotation> a) throws ClassNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String path = packageName.replace('.', '/');
        URL resource = loader.getResource(path);
        if (resource != null) {
            String filePath = resource.getFile();
            if (filePath != null && new File(filePath).isDirectory()) {
                for (String file : new File(filePath).list()) {
                    if (file.endsWith(".class")) {
                        String name = packageName + '.' + file.substring(0, file.indexOf(".class"));
                        Class<?> clazz = Class.forName(name);
                        if (clazz.isAnnotationPresent(a))
                            classes.add(clazz);
                    }
                    else if (new File(filePath,file).isDirectory()) {
                        recurse(classes, packageName + "." + file, a);
                    }
                }
            }
        }
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

    public static void verifyRejectionPolicy(String str) throws Exception{
        if(!(str.equalsIgnoreCase("run") || str.equalsIgnoreCase("abort")|| str.equalsIgnoreCase("discard")|| str.equalsIgnoreCase("discardoldest"))) {
            throw new Exception("Unknown rejection policy " + str);
        }
    }

    public static RejectedExecutionHandler parseRejectionPolicy(String rejection_policy) {
        if(rejection_policy == null)
            throw new IllegalArgumentException("rejection policy is null");
        if(rejection_policy.equalsIgnoreCase("abort"))
            return new ThreadPoolExecutor.AbortPolicy();
        if(rejection_policy.equalsIgnoreCase("discard"))
            return new ThreadPoolExecutor.DiscardPolicy();
        if(rejection_policy.equalsIgnoreCase("discardoldest"))
            return new ThreadPoolExecutor.DiscardOldestPolicy();
        if(rejection_policy.equalsIgnoreCase("run"))
            return new ThreadPoolExecutor.CallerRunsPolicy();
        throw new IllegalArgumentException("rejection policy \"" + rejection_policy + "\" not known");
    }


    /**
     * Parses comma-delimited longs; e.g., 2000,4000,8000.
     * Returns array of long, or null.
     */
    public static int[] parseCommaDelimitedInts(String s) {
        StringTokenizer tok;
        List<Integer> v=new ArrayList<Integer>();
        Integer l;
        int[] retval=null;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            l=new Integer(tok.nextToken());
            v.add(l);
        }
        if(v.isEmpty()) return null;
        retval=new int[v.size()];
        for(int i=0; i < v.size(); i++)
            retval[i]=v.get(i).intValue();
        return retval;
    }

    /**
     * Parses comma-delimited longs; e.g., 2000,4000,8000.
     * Returns array of long, or null.
     */
    public static long[] parseCommaDelimitedLongs(String s) {
        StringTokenizer tok;
        List<Long> v=new ArrayList<Long>();
        Long l;
        long[] retval=null;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            l=new Long(tok.nextToken());
            v.add(l);
        }
        if(v.isEmpty()) return null;
        retval=new long[v.size()];
        for(int i=0; i < v.size(); i++)
            retval[i]=v.get(i).longValue();
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
            for(int i=port;i <= port + port_range;i++) {
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
    public static List<InetSocketAddress> parseCommaDelimitedHosts2(String hosts, int port_range) {

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
        if(hostname == null) return null;

        int index=hostname.indexOf('.');
        if(index > 0 && !Character.isDigit(hostname.charAt(0)))
            return hostname.substring(0, index);
        else
            return hostname;
    }

    public static boolean startFlush(Channel c, List<Address> flushParticipants,
            int numberOfAttempts, long randomSleepTimeoutFloor, long randomSleepTimeoutCeiling) {
      int attemptCount = 0;
      while (attemptCount < numberOfAttempts) {
         try {
            c.startFlush(flushParticipants, false);
            return true;
         } catch (Exception e) {
            Util.sleepRandom(randomSleepTimeoutFloor, randomSleepTimeoutCeiling);
            attemptCount++;
         }
      }
      return false;
    }

    public static boolean startFlush(Channel c, List<Address> flushParticipants) {
    	return startFlush(c,flushParticipants,4,1000,5000);
    }

    public static boolean startFlush(Channel c, int numberOfAttempts, long randomSleepTimeoutFloor,long randomSleepTimeoutCeiling) {
        int attemptCount = 0;
        while(attemptCount < numberOfAttempts){
            try{
               c.startFlush(false);
               return true;
            } catch(Exception e) {
               Util.sleepRandom(randomSleepTimeoutFloor,randomSleepTimeoutCeiling);
               attemptCount++;
            }
        }
        return false;
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
            retval=shortName(InetAddress.getLocalHost().getHostName());
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


    public static <K,V> ConcurrentMap<K,V> createConcurrentMap(int initial_capacity, float load_factor, int concurrency_level) {
        return new ConcurrentHashMap<K,V>(initial_capacity, load_factor, concurrency_level);
    }

    public static <K,V> ConcurrentMap<K,V> createConcurrentMap(int initial_capacity) {
        return new ConcurrentHashMap<K,V>(initial_capacity);
    }

    public static <K,V> ConcurrentMap<K,V> createConcurrentMap() {
        return new ConcurrentHashMap<K,V>(CCHM_INITIAL_CAPACITY, CCHM_LOAD_FACTOR, CCHM_CONCURRENCY_LEVEL);
    }

    public static <K,V> Map<K,V> createHashMap() {
        return new HashMap<K,V>(CCHM_INITIAL_CAPACITY, CCHM_LOAD_FACTOR);
    }


  

    public static ServerSocket createServerSocket(SocketFactory factory, String service_name, InetAddress bind_addr, int start_port) {
        ServerSocket ret=null;

        while(true) {
            try {
                ret=factory.createServerSocket(service_name, start_port, 50, bind_addr);
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
     * Finds first available port starting at start_port and returns server
     * socket. Will not bind to port >end_port. Sets srv_port
     */
    public static ServerSocket createServerSocket(SocketFactory factory, String service_name, InetAddress bind_addr,
                                                  int start_port, int end_port) throws Exception {
        ServerSocket ret=null;
        int original_start_port=start_port;

        while(true) {
            try {
                if(bind_addr == null)
                    ret=factory.createServerSocket(service_name, start_port);
                else {
                    // changed (bela Sept 7 2007): we accept connections on all NICs
                    ret=factory.createServerSocket(service_name, start_port, 50, bind_addr);
                }
            }
            catch(SocketException bind_ex) {
                if(start_port == end_port)
                    throw new BindException("No available port to bind to in range [" + original_start_port + " .. " + end_port + "]");
                if(bind_addr != null && !bind_addr.isLoopbackAddress()) {
                    NetworkInterface nic=NetworkInterface.getByInetAddress(bind_addr);
                    if(nic == null)
                        throw new BindException("bind_addr " + bind_addr + " is not a valid interface: " + bind_ex);
                }
                start_port++;
                continue;
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
    public static DatagramSocket createDatagramSocket(SocketFactory factory, String service_name, InetAddress addr, int port) throws Exception {
        DatagramSocket sock=null;

        if(addr == null) {
            if(port == 0) {
                return factory.createDatagramSocket(service_name);
            }
            else {
                while(port < MAX_PORT) {
                    try {
                        return factory.createDatagramSocket(service_name, port);
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
                    return factory.createDatagramSocket(service_name, port, addr);
                }
                catch(BindException bind_ex) { // port already used
                    port++;
                }
            }
        }
        return sock; // will never be reached, but the stupid compiler didn't figure it out...
    }



    public static MulticastSocket createMulticastSocket(SocketFactory factory, String service_name, InetAddress mcast_addr, int port, Log log) throws IOException {
        if(mcast_addr != null && !mcast_addr.isMulticastAddress())
            throw new IllegalArgumentException("mcast_addr (" + mcast_addr + ") is not a valid multicast address");

        SocketAddress saddr=new InetSocketAddress(mcast_addr, port);
        MulticastSocket retval=null;

        try {
            retval=factory.createMulticastSocket(service_name, saddr);
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
            retval=factory.createMulticastSocket(service_name, port);
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

    	// determine the desired values for bind_addr_str and bind_interface_str
    	boolean ignore_systemprops=Util.isBindAddressPropertyIgnored();
    	String bind_addr_str =Util.getProperty(new String[]{Global.BIND_ADDR}, props, "bind_addr",
    			ignore_systemprops, null);
    	String bind_interface_str =Util.getProperty(new String[]{Global.BIND_INTERFACE, null}, props, "bind_interface",
    			ignore_systemprops, null);
    	
    	InetAddress bind_addr=null;
    	NetworkInterface bind_intf=null;

        StackType ip_version=Util.getIpStackType();

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
            bind_addr = getAddress(bind_intf, AddressScope.NON_LOOPBACK) ;
    	}
    	// 5. if neither bind address nor bind interface is specified, get the first non-loopback
    	// address on any interface
    	else if (bind_addr == null) {
    		bind_addr = getNonLoopbackAddress() ;
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
    	NetworkInterface bind_intf=null;

        if(bind_addr != null && bind_addr.isLoopbackAddress())
            return bind_addr;

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
    		bind_addr = getAddress(bind_intf, AddressScope.NON_LOOPBACK) ;
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



    /** IP related utilities */
    public static InetAddress getLocalhost(StackType ip_version) throws UnknownHostException {
    	if (ip_version == StackType.IPv4)
    		return InetAddress.getByName("127.0.0.1") ;
    	else
    		return InetAddress.getByName("::1") ;
    }



    /**
     * Returns the first non-loopback address on any interface on the current host.
     */
    public static InetAddress getNonLoopbackAddress() throws SocketException {
        return getAddress(AddressScope.NON_LOOPBACK);
    }



    /**
     * Returns the first address on any interface of the current host, which satisfies scope
     */
    public static InetAddress getAddress(AddressScope scope) throws SocketException {
        InetAddress address=null ;

        Enumeration intfs=NetworkInterface.getNetworkInterfaces();
        while(intfs.hasMoreElements()) {
            NetworkInterface intf=(NetworkInterface)intfs.nextElement();
            try {
                if(intf.isUp()) {
                    address=getAddress(intf, scope) ;
                    if(address != null)
                        return address;
                }
            }
            catch (SocketException e) {
            }
        }
        return null ;
    }


    /**
     * Returns the first address on the given interface on the current host, which satisfies scope
     *
     * @param intf the interface to be checked
     */
    public static InetAddress getAddress(NetworkInterface intf, AddressScope scope) {
        StackType ip_version=Util.getIpStackType();
        for(Enumeration addresses=intf.getInetAddresses(); addresses.hasMoreElements();) {
            InetAddress addr=(InetAddress)addresses.nextElement();
            boolean match;
            switch(scope) {
                case GLOBAL:
                    match=!addr.isLoopbackAddress() && !addr.isLinkLocalAddress() && !addr.isSiteLocalAddress();
                    break;
                case SITE_LOCAL:
                    match=addr.isSiteLocalAddress();
                    break;
                case LINK_LOCAL:
                    match=addr.isLinkLocalAddress();
                    break;
                case LOOPBACK:
                    match=addr.isLoopbackAddress();
                    break;
                case NON_LOOPBACK:
                    match=!addr.isLoopbackAddress();
                    break;
                default:
                    throw new IllegalArgumentException("scope " + scope + " is unknown");
            }

            if(match) {
                if((addr instanceof Inet4Address && ip_version == StackType.IPv4) ||
                        (addr instanceof Inet6Address && ip_version == StackType.IPv6))
                    return addr;
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
    public static boolean interfaceHasIPAddresses(NetworkInterface intf, StackType ip_version) throws UnknownHostException {
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
        
    public static StackType getIpStackType() {
       return ip_stack_type;
    }

    /**
     * Tries to determine the type of IP stack from the available interfaces and their addresses and from the
     * system properties (java.net.preferIPv4Stack and java.net.preferIPv6Addresses)
     * @return StackType.IPv4 for an IPv4 only stack, StackYTypeIPv6 for an IPv6 only stack, and StackType.Unknown
     * if the type cannot be detected
     */
    private static StackType _getIpStackType() {
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

    public static void checkIfValidAddress(InetAddress bind_addr, String prot_name) throws Exception {
        if(bind_addr.isAnyLocalAddress() || bind_addr.isLoopbackAddress())
            return;
        Collection<InetAddress> addrs=getAllAvailableAddresses();
        for(InetAddress addr: addrs) {
            if(addr.equals(bind_addr))
                return;
        }
        throw new BindException("[" + prot_name + "] " + bind_addr + " is not a valid address on any local network interface");
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
            if(tmp == null)
                return false;
            tmp=tmp.trim().toLowerCase();
            return !(tmp.equals("false") || tmp.equals("no") || tmp.equals("off")) && (tmp.equals("true") || tmp.equals("yes") || tmp.equals("on"));
        }
        catch(SecurityException ex) {
            return false;
        }
    }



    public static boolean isCoordinator(JChannel ch) {
        return isCoordinator(ch.getView(), ch.getAddress());
    }


    public static boolean isCoordinator(View view, Address local_addr) {
        if(view == null || local_addr == null)
            return false;
        List<Address> mbrs=view.getMembers();
        return !(mbrs == null || mbrs.isEmpty()) && local_addr.equals(mbrs.iterator().next());
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




