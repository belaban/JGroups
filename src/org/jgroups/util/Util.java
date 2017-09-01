package org.jgroups.util;

import org.jgroups.*;
import org.jgroups.TimeoutException;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
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
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.nanoTime;
import static org.jgroups.protocols.TP.MULTICAST;


/**
 * Collection of various utility routines that can not be assigned to other classes.
 * @author Bela Ban
 */
public class Util {

    private static final NumberFormat f;

    private static final Map<Class<? extends Object>,Byte> PRIMITIVE_TYPES=new HashMap<>(15);
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
    private static final byte TYPE_STRING       = 18; // ascii
    private static final byte TYPE_BYTEARRAY    = 19;
    // private static final byte TYPE_EXCEPTION    = 20;
    private static final byte TYPE_UTF_STRING   = 21; // multibyte charset

    // constants
    public static final int MAX_PORT=65535; // highest port allocatable
    @Deprecated  static boolean resolve_dns=false;

    private static final Pattern METHOD_NAME_TO_ATTR_NAME_PATTERN=Pattern.compile("[A-Z]+");
    private static final Pattern ATTR_NAME_TO_METHOD_NAME_PATTERN=Pattern.compile("_.");


    protected static int    CCHM_INITIAL_CAPACITY=16;
    protected static float  CCHM_LOAD_FACTOR=0.75f;
    protected static int    CCHM_CONCURRENCY_LEVEL=16;
    public static final int DEFAULT_HEADERS;

    /**
     * The max size of an address list, e.g. used in View or Digest when toString() is called. Limiting this
     * reduces the amount of log data
     */
    public static int MAX_LIST_PRINT_SIZE=20;

    private static final byte[] TYPE_NULL_ARRAY={0};
    private static final byte[] TYPE_BOOLEAN_TRUE={TYPE_BOOLEAN, 1};
    private static final byte[] TYPE_BOOLEAN_FALSE={TYPE_BOOLEAN, 0};

    public static final Class<?>[] getUnicastProtocols() {
        return new Class<?>[]{UNICAST.class,UNICAST2.class,UNICAST3.class};
    }

    public enum AddressScope {GLOBAL,SITE_LOCAL,LINK_LOCAL,LOOPBACK,NON_LOOPBACK}

    ;

    private static StackType ip_stack_type=_getIpStackType();


    protected static ResourceBundle resource_bundle;


    static {
        resource_bundle=ResourceBundle.getBundle("jg-messages",Locale.getDefault(),Util.class.getClassLoader());

        /* Trying to get value of resolve_dns. PropertyPermission not granted if
        * running in an untrusted environment with JNLP */
        try {
            resolve_dns=Boolean.valueOf(System.getProperty("resolve.dns","false"));
        }
        catch(SecurityException ex) {
            resolve_dns=false;
        }
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        // f.setMinimumFractionDigits(2);
        f.setMaximumFractionDigits(2);

        PRIMITIVE_TYPES.put(Boolean.class,TYPE_BOOLEAN);
        PRIMITIVE_TYPES.put(Byte.class,TYPE_BYTE);
        PRIMITIVE_TYPES.put(Character.class,TYPE_CHAR);
        PRIMITIVE_TYPES.put(Double.class,TYPE_DOUBLE);
        PRIMITIVE_TYPES.put(Float.class,TYPE_FLOAT);
        PRIMITIVE_TYPES.put(Integer.class,TYPE_INT);
        PRIMITIVE_TYPES.put(Long.class,TYPE_LONG);
        PRIMITIVE_TYPES.put(Short.class,TYPE_SHORT);
        PRIMITIVE_TYPES.put(String.class,TYPE_STRING);
        PRIMITIVE_TYPES.put(byte[].class,TYPE_BYTEARRAY);

        if(ip_stack_type == StackType.Unknown)
            ip_stack_type=StackType.IPv6;

        try {
            String cchm_initial_capacity=System.getProperty(Global.CCHM_INITIAL_CAPACITY);
            if(cchm_initial_capacity != null)
                CCHM_INITIAL_CAPACITY=Integer.valueOf(cchm_initial_capacity);
        }
        catch(SecurityException ex) {
        }

        try {
            String cchm_load_factor=System.getProperty(Global.CCHM_LOAD_FACTOR);
            if(cchm_load_factor != null)
                CCHM_LOAD_FACTOR=Float.valueOf(cchm_load_factor);
        }
        catch(SecurityException ex) {
        }

        try {
            String cchm_concurrency_level=System.getProperty(Global.CCHM_CONCURRENCY_LEVEL);
            if(cchm_concurrency_level != null)
                CCHM_CONCURRENCY_LEVEL=Integer.valueOf(cchm_concurrency_level);
        }
        catch(SecurityException ex) {
        }

        try {
            String tmp=System.getProperty(Global.MAX_LIST_PRINT_SIZE);
            if(tmp != null)
                MAX_LIST_PRINT_SIZE=Integer.valueOf(tmp);
        }
        catch(SecurityException ex) {
        }

        try {
            String tmp=System.getProperty(Global.DEFAULT_HEADERS);
            DEFAULT_HEADERS=tmp != null? new Integer(tmp) : 4;
        }
        catch(Throwable t) {
            throw new IllegalArgumentException(String.format("property %s has an incorrect value", Global.DEFAULT_HEADERS), t);
        }
    }


    public static void assertTrue(boolean condition) {
        assert condition;
    }

    public static void assertTrue(String message,boolean condition) {
        if(message != null)
            assert condition : message;
        else
            assert condition;
    }

    public static void assertFalse(boolean condition) {
        assertFalse(null,condition);
    }

    public static void assertFalse(String message,boolean condition) {
        if(message != null)
            assert !condition : message;
        else
            assert !condition;
    }


    public static void assertEquals(String message,Object val1,Object val2) {
        if(message != null) {
            assert val1.equals(val2) : message;
        }
        else {
            assert val1.equals(val2);
        }
    }

    public static void assertEquals(Object val1,Object val2) {
        assertEquals(null,val1,val2);
    }

    public static void assertNotNull(String message,Object val) {
        if(message != null)
            assert val != null : message;
        else
            assert val != null;
    }


    public static void assertNotNull(Object val) {
        assertNotNull(null, val);
    }


    public static void assertNull(String message,Object val) {
        if(message != null)
            assert val == null : message;
        else
            assert val == null;
    }


    public static int getNextHigherPowerOfTwo(int num) {
        if(num <= 0) return 1;
        int highestBit=Integer.highestOneBit(num);
        return num <= highestBit? highestBit : highestBit << 1;
    }


    public static String bold(String msg) {
        StringBuilder sb=new StringBuilder("\033[1m");
        sb.append(msg).append("\033[0m");
        return sb.toString();
    }

    public static String getMessage(String key) {
        return key != null? resource_bundle.getString(key) : null;
    }


    /**
     * Returns a default stack for testing with transport = SHARED_LOOPBACK
     * @param additional_protocols Any number of protocols to add to the top of the returned protocol list
     * @return
     */
    public static Protocol[] getTestStack(Protocol... additional_protocols) {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(1000),
          new FRAG2().fragSize(8000)
        };

        if(additional_protocols == null)
            return protocols;
        Protocol[] tmp=Arrays.copyOf(protocols,protocols.length + additional_protocols.length);
        System.arraycopy(additional_protocols, 0, tmp, protocols.length, additional_protocols.length);
        return tmp;
    }


    /**
     * Blocks until all channels have the same view
     * @param timeout  How long to wait (max in ms)
     * @param interval Check every interval ms
     * @param channels The channels which should form the view. The expected view size is channels.length.
     *                 Must be non-null
     */
    public static void waitUntilAllChannelsHaveSameView(long timeout, long interval, Channel... channels) throws TimeoutException {
        if(interval >= timeout || timeout <= 0)
            throw new IllegalArgumentException("interval needs to be smaller than timeout or timeout needs to be > 0");
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_channels_have_correct_view=true;
            View first=channels[0].getView();
            for(Channel ch : channels) {
                View view=ch.getView();
                if(!Objects.equals(view, first) || view.size() != channels.length) {
                    all_channels_have_correct_view=false;
                    break;
                }
            }
            if(all_channels_have_correct_view)
                return;
            Util.sleep(interval);
        }
        View[] views=new View[channels.length];
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < channels.length; i++) {
            views[i]=channels[i].getView();
            sb.append(channels[i].getName()).append(": ").append(views[i]).append("\n");
        }
        View first=channels[0].getView();
        for(View view : views)
            if(!Objects.equals(view, first))
                throw new TimeoutException("Timeout " + timeout + " kicked in, views are:\n" + sb);
    }

    public static boolean allChannelsHaveSameView(JChannel... channels) {
        View first=channels[0].getView();
        for(JChannel ch : channels) {
            View view=ch.getView();
            if(!Objects.equals(view, first) || view.size() != channels.length)
                return false;
        }
        return true;
    }

    public static void assertAllChannelsHaveSameView(JChannel ... channels) {
        assert allChannelsHaveSameView(channels) : String.format("channels have different views:\n%s\n", printViews(channels));
    }

    public static String printViews(JChannel ... channels) {
        StringBuilder sb=new StringBuilder();
        for(JChannel ch: channels)
            sb.append(String.format("%s: %s\n", ch.getName(), ch.getView()));
        return sb.toString();
    }



    /**
     * Waits until a list has the expected number of elements. Throws an exception if not met
     * @param list          The list
     * @param expected_size The expected size
     * @param timeout       The time to wait (in ms)
     * @param interval      The interval at which to get the size of the list (in ms)
     * @param <T>           The type of the list
     */
    public static <T> void waitUntilListHasSize(List<T> list,int expected_size,long timeout,long interval) {
        if(list == null)
            throw new IllegalStateException("list is null");
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < target_time) {
            if(list.size() == expected_size)
                break;
            Util.sleep(interval);
        }
        assert list.size() == expected_size : "list doesn't have the expected (" + expected_size + ") elements: " + list;
    }


    public static void setScope(Message msg,short scope) {
        SCOPE.ScopeHeader hdr=SCOPE.ScopeHeader.createMessageHeader(scope);
        msg.putHeader(Global.SCOPE_ID,hdr);
        msg.setFlag(Message.Flag.SCOPED);
    }

    public static short getScope(Message msg) {
        SCOPE.ScopeHeader hdr=(SCOPE.ScopeHeader)msg.getHeader(Global.SCOPE_ID);
        return hdr != null? hdr.getScope() : 0;
    }

    public static byte[] createAuthenticationDigest(String passcode,long t1,double q1) throws IOException,
                                                                                              NoSuchAlgorithmException {
        ByteArrayOutputStream baos=new ByteArrayOutputStream(512);
        DataOutputStream out=new DataOutputStream(baos);
        byte[] digest=createDigest(passcode,t1,q1);
        out.writeLong(t1);
        out.writeDouble(q1);
        out.writeInt(digest.length);
        out.write(digest);
        out.flush();
        return baos.toByteArray();
    }

    public static byte[] createDigest(String passcode,long t1,double q1)
      throws IOException, NoSuchAlgorithmException {
        MessageDigest md=MessageDigest.getInstance("SHA");
        md.update(passcode.getBytes());
        ByteBuffer bb=ByteBuffer.allocate(16); //8 bytes for long and double each
        bb.putLong(t1);
        bb.putDouble(q1);
        md.update(bb);
        return md.digest();
    }

    /**
     * Utility method. If the dest address is IPv6, convert scoped link-local addrs into unscoped ones
     * @param sock
     * @param dest
     * @param sock_conn_timeout
     * @throws IOException
     */
    public static void connect(Socket sock,SocketAddress dest,int sock_conn_timeout) throws IOException {
        if(dest instanceof InetSocketAddress) {
            InetAddress addr=((InetSocketAddress)dest).getAddress();
            if(addr instanceof Inet6Address) {
                Inet6Address tmp=(Inet6Address)addr;
                if(tmp.getScopeId() != 0) {
                    dest=new InetSocketAddress(InetAddress.getByAddress(tmp.getAddress()),((InetSocketAddress)dest).getPort());
                }
            }
        }
        sock.connect(dest, sock_conn_timeout);
    }

    public static boolean connect(SocketChannel ch, SocketAddress dest) throws IOException {
        if(dest instanceof InetSocketAddress) {
            InetAddress addr=((InetSocketAddress)dest).getAddress();
            if(addr instanceof Inet6Address) {
                Inet6Address tmp=(Inet6Address)addr;
                if(tmp.getScopeId() != 0) {
                    dest=new InetSocketAddress(InetAddress.getByAddress(tmp.getAddress()),((InetSocketAddress)dest).getPort());
                }
            }
        }
        return ch.connect(dest);
    }


    public static void close(Closeable closeable) {
        if(closeable != null) {
            try {
                closeable.close();
            }
            catch(Throwable t) {
            }
        }
    }


    public static void close(Closeable... closeables) {
        if(closeables != null) {
            for(Closeable closeable : closeables)
                Util.close(closeable);
        }
    }


    /**
     * Drops messages to/from other members and then closes the channel. Note that this member won't get excluded from
     * the view until failure detection has kicked in and the new coord installed the new view
     */
    public static void shutdown(Channel ch) throws Exception {
        DISCARD discard=new DISCARD();
        discard.setLocalAddress(ch.getAddress());
        discard.setDiscardAll(true);
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        stack.insertProtocol(discard,ProtocolStack.ABOVE,transport.getClass());

        //abruptly shutdown FD_SOCK just as in real life when member gets killed non gracefully
        FD_SOCK fd=(FD_SOCK)ch.getProtocolStack().findProtocol(FD_SOCK.class);
        if(fd != null)
            fd.stopServerSocket(false);

        View view=ch.getView();
        if(view != null) {
            ViewId vid=view.getViewId();
            List<Address> members=Collections.singletonList(ch.getAddress());

            ViewId new_vid=new ViewId(ch.getAddress(),vid.getId() + 1);
            View new_view=new View(new_vid,members);

            // inject view in which the shut down member is the only element
            GMS gms=(GMS)stack.findProtocol(GMS.class);
            gms.installView(new_view);
        }
        Util.close(ch);
    }


    public static byte setFlag(byte bits,byte flag) {
        return bits|=flag;
    }


    public static boolean isFlagSet(byte bits,byte flag) {
        return (bits & flag) == flag;
    }


    public static byte clearFlags(byte bits,byte flag) {
        return bits&=~flag;
    }


    /**
     * Creates an object from a byte buffer
     */
    public static Object objectFromByteBuffer(byte[] buffer) throws Exception {
        if(buffer == null) return null;
        return objectFromByteBuffer(buffer,0,buffer.length);
    }

    public static Object objectFromByteBuffer(byte[] buffer,int offset,int length) throws Exception {
        return objectFromByteBuffer(buffer, offset, length, null);
    }


    public static Object objectFromByteBuffer(byte[] buffer,int offset,int length, ClassLoader loader) throws Exception {
        if(buffer == null) return null;
        byte type=buffer[offset++];
        length--;
        switch(type) {
            case TYPE_NULL:    return null;
            case TYPE_STREAMABLE:
                DataInput in=new ByteArrayDataInputStream(buffer,offset,length);
                return readGenericStreamable(in, loader);
            case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                InputStream in_stream=new ByteArrayInputStream(buffer,offset,length);
                try(ObjectInputStream oin=new ObjectInputStreamWithClassloader(in_stream, loader)) {
                    return oin.readObject();
                }
            case TYPE_BOOLEAN: return (Boolean)(buffer[offset] == 1);
            case TYPE_BYTE:    return (Byte)buffer[offset];
            case TYPE_CHAR:    return (Character)Bits.readChar(buffer, offset);
            case TYPE_DOUBLE:  return (Double)Bits.readDouble(buffer, offset);
            case TYPE_FLOAT:   return (Float)Bits.readFloat(buffer, offset);
            case TYPE_INT:     return (Integer)Bits.readInt(buffer, offset);
            case TYPE_LONG:    return (Long)Bits.readLong(buffer, offset);
            case TYPE_SHORT:   return (Short)Bits.readShort(buffer, offset);
            case TYPE_STRING:  return new String(buffer, offset, length);
            case TYPE_UTF_STRING:
                in=new ByteArrayDataInputStream(buffer, offset, length);
                return in.readUTF();
            case TYPE_BYTEARRAY:
                byte[] tmp=new byte[length];
                System.arraycopy(buffer,offset,tmp,0,length);
                return tmp;
            default:
                throw new IllegalArgumentException("type " + type + " is invalid");
        }
    }


    /**
     * Serializes/Streams an object into a byte buffer.
     * The object has to implement interface Serializable or Externalizable or Streamable.
     */
    public static byte[] objectToByteBuffer(Object obj) throws Exception {
        if(obj == null)
            return TYPE_NULL_ARRAY;

        if(obj instanceof Streamable) {
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512, true);
            out.write(TYPE_STREAMABLE);
            writeGenericStreamable((Streamable)obj,out);
            return Arrays.copyOf(out.buf,out.position());
        }

        Byte type=PRIMITIVE_TYPES.get(obj.getClass());
        if(type == null) { // will throw an exception if object is not serializable
            final ByteArrayDataOutputStream out_stream=new ByteArrayDataOutputStream(512, true);
            out_stream.write(TYPE_SERIALIZABLE);
            try(ObjectOutputStream out=new ObjectOutputStream(new OutputStreamAdapter(out_stream))) {
                out.writeObject(obj);
                out.flush();
                return Arrays.copyOf(out_stream.buffer(), out_stream.position());
            }
        }

        return marshalPrimitiveType(type, obj);
    }


    public static Buffer objectToBuffer(Object obj) throws Exception {
        if(obj == null)
            return new Buffer(TYPE_NULL_ARRAY);

        if(obj instanceof Streamable) {
            final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512, true);
            out.write(TYPE_STREAMABLE);
            writeGenericStreamable((Streamable)obj,out);
            return out.getBuffer();
        }

        Byte type=PRIMITIVE_TYPES.get(obj.getClass());
        if(type == null) { // will throw an exception if object is not serializable
            final ByteArrayDataOutputStream out_stream=new ByteArrayDataOutputStream(512, true);
            out_stream.write(TYPE_SERIALIZABLE);
            try(ObjectOutputStream out=new ObjectOutputStream(new OutputStreamAdapter(out_stream))) {
                out.writeObject(obj);
                out.flush();
                return out_stream.getBuffer();
            }
        }
        return new Buffer(marshalPrimitiveType(type, obj));
    }


    protected static byte[] marshalPrimitiveType(byte type, Object obj) {
          switch(type) {
              case TYPE_BOOLEAN:
                  return ((Boolean)obj)? TYPE_BOOLEAN_TRUE : TYPE_BOOLEAN_FALSE;
              case TYPE_BYTE:
                  return new byte[]{TYPE_BYTE, (byte)obj};
              case TYPE_CHAR:
                  byte[] buf=new byte[Global.BYTE_SIZE *3];
                  buf[0]=TYPE_CHAR;
                  Bits.writeChar((char)obj, buf, 1);
                  return buf;
              case TYPE_DOUBLE:
                  buf=new byte[Global.BYTE_SIZE + Global.DOUBLE_SIZE];
                  buf[0]=TYPE_DOUBLE;
                  Bits.writeDouble((double)obj, buf, 1);
                  return buf;
              case TYPE_FLOAT:
                  buf=new byte[Global.BYTE_SIZE + Global.FLOAT_SIZE];
                  buf[0]=TYPE_FLOAT;
                  Bits.writeFloat((float)obj, buf, 1);
                  return buf;
              case TYPE_INT:
                  buf=new byte[Global.BYTE_SIZE + Global.INT_SIZE];
                  buf[0]=TYPE_INT;
                  Bits.writeInt((int)obj, buf, 1);
                  return buf;
              case TYPE_LONG:
                  buf=new byte[Global.BYTE_SIZE + Global.LONG_SIZE];
                  buf[0]=TYPE_LONG;
                  Bits.writeLong((long)obj, buf, 1);
                  return buf;
              case TYPE_SHORT:
                  buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE];
                  buf[0]=TYPE_SHORT;
                  Bits.writeShort((short)obj, buf, 1);
                  return buf;
              case TYPE_STRING:
                  String str=(String)obj;
                  if(Util.isAsciiString(str)) {
                      int len=str.length();
                      ByteBuffer retval=ByteBuffer.allocate(Global.BYTE_SIZE + len).put(TYPE_STRING);
                      for(int i=0; i < len; i++)
                          retval.put((byte)str.charAt(i));
                      return retval.array();
                  }
                  else {
                      ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(str.length()*2 +3);
                      out.write(TYPE_UTF_STRING);
                      out.writeUTF(str);
                      byte[] ret=new byte[out.position()];
                      System.arraycopy(out.buffer(), 0, ret, 0, ret.length);
                      return ret;
                  }
              case TYPE_BYTEARRAY:
                  buf=(byte[])obj;
                  byte[] buffer=new byte[Global.BYTE_SIZE + buf.length];
                  buffer[0]=TYPE_BYTEARRAY;
                  System.arraycopy(buf, 0, buffer, 1, buf.length);
                  return buffer;
              default:
                  throw new IllegalArgumentException("type " + type + " is invalid");
          }
      }



    public static void objectToStream(Object obj,DataOutput out) throws Exception {
        if(obj == null) {
            out.write(TYPE_NULL);
            return;
        }

        Byte type;
        if(obj instanceof Streamable) {  // use Streamable if we can
            out.write(TYPE_STREAMABLE);
            writeGenericStreamable((Streamable)obj,out);
        }
        else if((type=PRIMITIVE_TYPES.get(obj.getClass())) != null) {
            if(type != TYPE_STRING)
                out.write(type);
            switch(type) {
                case TYPE_BOOLEAN:
                    out.writeBoolean((Boolean)obj);
                    break;
                case TYPE_BYTE:
                    out.writeByte((Byte)obj);
                    break;
                case TYPE_CHAR:
                    out.writeChar((Character)obj);
                    break;
                case TYPE_DOUBLE:
                    out.writeDouble((Double)obj);
                    break;
                case TYPE_FLOAT:
                    out.writeFloat((Float)obj);
                    break;
                case TYPE_INT:
                    out.writeInt((Integer)obj);
                    break;
                case TYPE_LONG:
                    out.writeLong((Long)obj);
                    break;
                case TYPE_SHORT:
                    out.writeShort((Short)obj);
                    break;
                case TYPE_STRING:
                    String str=(String)obj;
                    if(Util.isAsciiString(str)) {
                        int len=str.length();
                        out.write(TYPE_STRING);
                        out.writeInt(len);
                        for(int i=0; i < len; i++)
                            out.write((byte)str.charAt(i));
                    }
                    else {
                        out.write(TYPE_UTF_STRING);
                        out.writeUTF(str);
                    }
                    break;
                case TYPE_BYTEARRAY:
                    byte[] buf=(byte[])obj;
                    out.writeInt(buf.length);
                    out.write(buf,0,buf.length);
                    break;
                default:
                    throw new IllegalArgumentException("type " + type + " is invalid");
            }
        }
        else { // will throw an exception if object is not serializable
            out.write(TYPE_SERIALIZABLE);
            ObjectOutputStream tmp=new ObjectOutputStream(out instanceof ByteArrayDataOutputStream?
                                                            new OutputStreamAdapter((ByteArrayDataOutputStream)out) :
                                                            (OutputStream)out);
            try {
                tmp.writeObject(obj);
            }
            finally {
                Util.close(tmp);
            }
        }
    }

    public static Object objectFromStream(DataInput in) throws Exception {
        return objectFromStream(in, null);
    }

    public static Object objectFromStream(DataInput in, ClassLoader loader) throws Exception {
        if(in == null) return null;
        byte b=in.readByte();

        switch(b) {
            case TYPE_NULL:       return null;
            case TYPE_STREAMABLE: return readGenericStreamable(in, loader);
            case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                InputStream is=in instanceof ByteArrayDataInputStream?
                  new org.jgroups.util.InputStreamAdapter((ByteArrayDataInputStream)in) : (InputStream)in;
                try(ObjectInputStream tmp=new ObjectInputStreamWithClassloader(is, loader)) {
                    return tmp.readObject();
                }
            case TYPE_BOOLEAN:    return in.readBoolean();
            case TYPE_BYTE:       return in.readByte();
            case TYPE_CHAR:       return in.readChar();
            case TYPE_DOUBLE:     return in.readDouble();
            case TYPE_FLOAT:      return in.readFloat();
            case TYPE_INT:        return in.readInt();
            case TYPE_LONG:       return in.readLong();
            case TYPE_SHORT:      return in.readShort();
            case TYPE_STRING:
                int str_len=in.readInt();
                if(str_len == 0) return "";
                byte[] tmp=new byte[str_len];
                for(int i=0; i < str_len; i++) {
                    tmp[i]=in.readByte();
                }
                return new String(tmp);
            case TYPE_UTF_STRING:
                return in.readUTF();
            case TYPE_BYTEARRAY:
                int len=in.readInt();
                byte[] tmpbuf=new byte[len];
                in.readFully(tmpbuf,0,tmpbuf.length);
                return tmpbuf;
            default:
                throw new IllegalArgumentException("type " + b + " is invalid");
        }
    }


    public static Streamable streamableFromByteBuffer(Class<? extends Streamable> cl,byte[] buffer) throws Exception {
        return streamableFromByteBuffer(cl,buffer,0,buffer.length);
    }


    public static void bufferToArray(final Address sender, final ByteBuffer buf, org.jgroups.blocks.cs.Receiver target) {
        if(buf == null)
            return;
        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(),
          len=buf.remaining();
        if(!buf.isDirect())
            target.receive(sender, buf.array(), offset, len);
        else { // by default use a copy; but of course implementers of Receiver can override this
            byte[] tmp=new byte[len];
            buf.get(tmp, 0, len);
            target.receive(sender, tmp, 0, len);
        }
    }


    public static <T extends Streamable> Streamable streamableFromByteBuffer(Class<? extends Streamable> cl,byte[] buffer,int offset,int length) throws Exception {
        if(buffer == null) return null;
        DataInput in=new ByteArrayDataInputStream(buffer,offset,length);
        T retval=(T)cl.newInstance();
        retval.readFrom(in);
        return retval;
    }


    public static <T extends Streamable> T streamableFromBuffer(Class<T> clazz,byte[] buffer,int offset,int length) throws Exception {
        DataInput in=new ByteArrayDataInputStream(buffer,offset,length);
        return (T)Util.readStreamable(clazz,in);
    }


    public static byte[] streamableToByteBuffer(Streamable obj) throws Exception {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
        obj.writeTo(out);
        return Arrays.copyOf(out.buffer(), out.position());
    }

    public static Buffer streamableToBuffer(Streamable obj) {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
        try {
            Util.writeStreamable(obj,out);
            return out.getBuffer();
        }
        catch(Exception ex) {
            return null;
        }
    }


    public static byte[] collectionToByteBuffer(Collection<Address> c) throws Exception {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
        Util.writeAddresses(c,out);
        return Arrays.copyOf(out.buffer(), out.position());
    }

    public static byte[] stringToBytes(String str) {
        if(str == null) return null;
        byte[] retval=new byte[str.length()];
        for(int i=0; i < retval.length; i++)
            retval[i]=(byte)str.charAt(i);
        return retval;
    }

    public static String bytesToString(byte[] bytes) {
        return bytes != null? new String(bytes) : null;
    }

    public static String byteArrayToHexString(byte[] b) {
        if(b == null)
            return "null";
        return byteArrayToHexString(b, 0, b.length);
    }

    public static String byteArrayToHexString(byte[] b, int offset, int length) {
        if(b == null)
            return "null";
        StringBuilder sb = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++){
            int v = b[i+offset] & 0xff;
            if (v < 16) { sb.append('0'); }
            sb.append(Integer.toHexString(v));
        }
        return sb.toString().toUpperCase();
    }

    public static boolean isAsciiString(String str) {
        if(str == null) return false;
        for(int i=0; i < str.length(); i++) {
            int ch=str.charAt(i);
            if(ch >= 128)
                return false;
        }
        return true;
    }

    /** Compares 2 byte arrays, elements are treated as unigned */
    public static int compare(byte[] left,byte[] right) {
        for(int i=0, j=0; i < left.length && j < right.length; i++,j++) {
            int a=(left[i] & 0xff);
            int b=(right[j] & 0xff);
            if(a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    /**
     * This method needs to be synchronized on out_stream when it is called
     * @param msg
     * @throws java.io.IOException
     */
    public static void writeMessage(Message msg, DataOutput dos, boolean multicast) throws Exception {
        byte flags=0;
        dos.writeShort(Version.version); // write the version
        if(multicast)
            flags+=MULTICAST;
        dos.writeByte(flags);
        msg.writeTo(dos);
    }

    public static Message readMessage(DataInput instream) throws Exception {
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(instream);
        return msg;
    }




    /**
     * Write a list of messages with the *same* destination and src addresses. The message list is
     * marshalled as follows (see doc/design/MarshallingFormat.txt for details):
     * <pre>
     * List: * | version | flags | dest | src | cluster-name | [Message*] |
     *
     * Message:  | presence | leading | flags | [src] | length | [buffer] | size | [Headers*] |
     *
     * </pre>
     * @param dest
     * @param src
     * @param msgs
     * @param dos
     * @param multicast
     * @throws Exception
     */
    public static void writeMessageList(Address dest, Address src, byte[] cluster_name,
                                        List<Message> msgs, DataOutput dos, boolean multicast, short transport_id) throws Exception {
        writeMessageListHeader(dest, src, cluster_name, msgs != null ? msgs.size() : 0, dos, multicast);

        if(msgs != null)
            for(Message msg: msgs)
                msg.writeToNoAddrs(src, dos, transport_id); // exclude the transport header
    }

    public static void writeMessageListHeader(Address dest, Address src, byte[] cluster_name, int numMsgs, DataOutput dos, boolean multicast) throws Exception {
        dos.writeShort(Version.version);

        byte flags=TP.LIST;
        if(multicast)
            flags+=TP.MULTICAST;

        dos.writeByte(flags);

        Util.writeAddress(dest, dos);

        Util.writeAddress(src, dos);

        dos.writeShort(cluster_name != null? cluster_name.length : -1);
        if(cluster_name != null)
            dos.write(cluster_name);

        dos.writeInt(numMsgs);
    }


    public static List<Message> readMessageList(DataInput in, short transport_id) throws Exception {
        List<Message> list=new LinkedList<>();
        Address dest=Util.readAddress(in);
        Address src=Util.readAddress(in);
        // AsciiString cluster_name=Bits.readAsciiString(in); // not used here
        short length=in.readShort();
        byte[] cluster_name=length >= 0? new byte[length] : null;
        if(cluster_name != null)
            in.readFully(cluster_name, 0, cluster_name.length);

        int len=in.readInt();

        for(int i=0; i < len; i++) {
            Message msg=new Message(false);
            msg.readFrom(in);
            msg.setDest(dest);
            if(msg.getSrc() == null)
                msg.setSrc(src);

            // Now add a TpHeader back on, was not marshalled. Every message references the *same* TpHeader, saving memory !
            msg.putHeader(transport_id, new TpHeader(cluster_name));

            list.add(msg);
        }
        return list;
    }

    /**
     * Reads a list of messages into 4 MessageBatches:
     * <ol>
     *     <li>regular</li>
     *     <li>OOB</li>
     *     <li>INTERNAL-OOB (INTERNAL and OOB)</li>
     *     <li>INTERNAL (INTERNAL)</li>
     * </ol>
     * @param in
     * @return an array of 4 MessageBatches in the order above, the first batch is at index 0
     * @throws Exception
     */
    public static MessageBatch[] readMessageBatch(DataInput in, boolean multicast) throws Exception {
        MessageBatch[] batches=new MessageBatch[4]; // [0]: reg, [1]: OOB, [2]: internal-oob, [3]: internal
        Address dest=Util.readAddress(in);
        Address src=Util.readAddress(in);
        // AsciiString cluster_name=Bits.readAsciiString(in);
        short length=in.readShort();
        byte[] cluster_name=length >= 0? new byte[length] : null;
        if(cluster_name != null)
            in.readFully(cluster_name, 0, cluster_name.length);

        int len=in.readInt();
        for(int i=0; i < len; i++) {
            Message msg=new Message(false);
            msg.readFrom(in);
            msg.setDest(dest);
            if(msg.getSrc() == null)
                msg.setSrc(src);
            boolean oob=msg.isFlagSet(Message.Flag.OOB);
            boolean internal=msg.isFlagSet(Message.Flag.INTERNAL);
            int index=0;
            MessageBatch.Mode mode=MessageBatch.Mode.REG;

            if(oob && !internal) {
                index=1; mode=MessageBatch.Mode.OOB;
            }
            else if(oob && internal) {
                index=2; mode=MessageBatch.Mode.OOB;
            }
            else if(!oob && internal) {
                index=3; mode=MessageBatch.Mode.INTERNAL;
            }

            if(batches[index] == null)
                batches[index]=new MessageBatch(dest, src, cluster_name != null? new AsciiString(cluster_name) : null, multicast, mode, len);
            batches[index].add(msg);
        }
        return batches;
    }


    public static void writeView(View view,DataOutput out) throws Exception {
        if(view == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeBoolean(view instanceof MergeView);
        view.writeTo(out);
    }


    public static View readView(DataInput in) throws Exception {
        if(!in.readBoolean())
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

    public static void writeViewId(ViewId vid,DataOutput out) throws Exception {
        if(vid == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        vid.writeTo(out);
    }

    public static ViewId readViewId(DataInput in) throws Exception {
        if(!in.readBoolean())
            return null;
        ViewId retval=new ViewId();
        retval.readFrom(in);
        return retval;
    }


    public static void writeAddress(Address addr,DataOutput out) throws Exception {
        byte flags=0;
        boolean streamable_addr=true;

        if(addr == null) {
            flags=Util.setFlag(flags,Address.NULL);
            out.writeByte(flags);
            return;
        }

        if(addr instanceof UUID) {
            Class<? extends Address> clazz=addr.getClass();
            if(clazz.equals(UUID.class))
                flags=Util.setFlag(flags,Address.UUID_ADDR);
            else if(clazz.equals(SiteUUID.class))
                flags=Util.setFlag(flags,Address.SITE_UUID);
            else if(clazz.equals(SiteMaster.class))
                flags=Util.setFlag(flags,Address.SITE_MASTER);
            else
                streamable_addr=false;
        }
        else if(addr instanceof IpAddress)
            flags=Util.setFlag(flags,Address.IP_ADDR);
        else
            streamable_addr=false;
        out.writeByte(flags);
        if(streamable_addr)
            addr.writeTo(out);
        else
            writeOtherAddress(addr,out);
    }

    public static Address readAddress(DataInput in) throws Exception {
        byte flags=in.readByte();
        if(Util.isFlagSet(flags,Address.NULL))
            return null;

        Address addr;
        if(Util.isFlagSet(flags,Address.UUID_ADDR)) {
            addr=new UUID();
            addr.readFrom(in);
        }
        else if(Util.isFlagSet(flags,Address.SITE_UUID)) {
            addr=new SiteUUID();
            addr.readFrom(in);
        }
        else if(Util.isFlagSet(flags,Address.SITE_MASTER)) {
            addr=new SiteMaster();
            addr.readFrom(in);
        }
        else if(Util.isFlagSet(flags,Address.IP_ADDR)) {
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
        if(addr == null)
            return retval;

        if(addr instanceof UUID) {
            Class<? extends Address> clazz=addr.getClass();
            if(clazz.equals(UUID.class) || clazz.equals(SiteUUID.class) || clazz.equals(SiteMaster.class))
                return retval + addr.size();
        }
        if(addr instanceof IpAddress)
            return retval + addr.size();

        retval+=Global.SHORT_SIZE; // magic number
        retval+=addr.size();
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
            retval+=s.length() + 2;
        return retval;
    }

    public static int size(byte[] buf) {
       /* int retval=Global.BYTE_SIZE + Global.INT_SIZE;
        if(buf != null)
            retval+=buf.length;
        return retval;*/
        return buf == null? Global.BYTE_SIZE : Global.BYTE_SIZE + Global.INT_SIZE + buf.length;
    }

    private static Address readOtherAddress(DataInput in) throws Exception {
        short magic_number=in.readShort();
        Class<?> cl=ClassConfigurator.get(magic_number);
        if(cl == null)
            throw new RuntimeException("class for magic number " + magic_number + " not found");
        Address addr=(Address)cl.newInstance();
        addr.readFrom(in);
        return addr;
    }

    private static void writeOtherAddress(Address addr,DataOutput out) throws Exception {
        short magic_number=ClassConfigurator.getMagicNumber(addr.getClass());

        // write the class info
        if(magic_number == -1)
            throw new RuntimeException("magic number " + magic_number + " not found");

        out.writeShort(magic_number);
        addr.writeTo(out);
    }

    /**
     * Writes a list of Addresses. Can contain 65K addresses at most
     * @param v   A Collection<Address>
     * @param out
     * @throws Exception
     */
    public static void writeAddresses(Collection<? extends Address> v,DataOutput out) throws Exception {
        if(v == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(v.size());
        for(Address addr : v) {
            Util.writeAddress(addr,out);
        }
    }

    public static void writeAddresses(final Address[] addrs,DataOutput out) throws Exception {
        if(addrs == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(addrs.length);
        for(Address addr : addrs)
            Util.writeAddress(addr,out);
    }

    /**
     * @param in
     * @param cl The type of Collection, e.g. ArrayList.class
     * @return Collection of Address objects
     * @throws Exception
     */
    public static Collection<? extends Address> readAddresses(DataInput in,Class cl) throws Exception {
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


    public static Address[] readAddresses(DataInput in) throws Exception {
        short length=in.readShort();
        if(length < 0) return null;
        Address[] retval=new Address[length];
        for(int i=0; i < length; i++) {
            Address addr=Util.readAddress(in);
            retval[i]=addr;
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

    public static long size(Address[] addrs) {
        int retval=Global.SHORT_SIZE; // number of elements
        if(addrs != null)
            for(Address addr : addrs)
                retval+=Util.size(addr);
        return retval;
    }


    public static void writeStreamable(Streamable obj,DataOutput out) throws Exception {
        if(obj == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        obj.writeTo(out);
    }


    public static Streamable readStreamable(Class clazz,DataInput in) throws Exception {
        Streamable retval=null;
        if(!in.readBoolean())
            return null;
        retval=(Streamable)clazz.newInstance();
        retval.readFrom(in);
        return retval;
    }


    public static void writeGenericStreamable(Streamable obj,DataOutput out) throws Exception {
        short magic_number;
        String classname;

        if(obj == null) {
            out.write(0);
            return;
        }

        out.write(1);
        magic_number=ClassConfigurator.getMagicNumber(obj.getClass());
        out.writeShort(magic_number);
        if(magic_number == -1) {
            classname=obj.getClass().getName();
            out.writeUTF(classname);
        }
        obj.writeTo(out); // write the contents
    }

    public static Streamable readGenericStreamable(DataInput in) throws Exception {
        return readGenericStreamable(in, null);
    }

    public static Streamable readGenericStreamable(DataInput in, ClassLoader loader) throws Exception {
        Streamable retval=null;
        int b=in.readByte();
        if(b == 0)
            return null;

        short magic_number=in.readShort();

        Class<?> clazz;
        if(magic_number != -1) {
            clazz=ClassConfigurator.get(magic_number);
            if(clazz == null)
                throw new ClassNotFoundException("Class for magic number " + magic_number + " cannot be found.");
        }
        else {
            String classname=in.readUTF();
            clazz=ClassConfigurator.get(classname, loader);
        }

        retval=(Streamable)clazz.newInstance();
        retval.readFrom(in);
        return retval;
    }


    public static <T extends Streamable> void write(T[] array, DataOutput out) throws Exception {
        Bits.writeInt(array != null? array.length : 0, out);
        if(array == null)
            return;
        for(T el: array)
            el.writeTo(out);
    }

    public static <T extends Streamable> T[] read(Class<T> clazz, DataInput in) throws Exception {
        int size=Bits.readInt(in);
        if(size == 0)
            return null;
        T[] retval=(T[])Array.newInstance(clazz, size);

        for(int i=0; i < retval.length; i++) {
            retval[i]=clazz.newInstance();
            retval[i].readFrom(in);
        }
        return retval;
    }

    public static void writeClass(Class<?> classObject,DataOutput out) throws Exception {
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
        boolean use_magic_number=in.readBoolean();
        if(use_magic_number) {
            short magic_number=in.readShort();
            clazz=ClassConfigurator.get(magic_number);
            if(clazz == null)
                throw new ClassNotFoundException("Class for magic number " + magic_number + " cannot be found.");
        }
        else {
            String classname=in.readUTF();
            clazz=ClassConfigurator.get(classname);
        }

        return clazz;
    }

    public static void writeObject(Object obj,DataOutput out) throws Exception {
        if(obj instanceof Streamable) {
            out.writeInt(-1);
            writeGenericStreamable((Streamable)obj,out);
        }
        else {
            byte[] buf=objectToByteBuffer(obj);
            out.writeInt(buf.length);
            out.write(buf,0,buf.length);
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


    public static String readFile(String filename) throws FileNotFoundException {
        FileInputStream in=new FileInputStream(filename);
        try {
            return readContents(in);
        }
        finally {
            Util.close(in);
        }
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

    public static byte[] readFileContents(InputStream input) throws IOException {
        byte contents[]=new byte[10000], buf[]=new byte[1024];
        InputStream in=new BufferedInputStream(input);
        int bytes_read=0;

        for(; ; ) {
            int tmp=in.read(buf,0,buf.length);
            if(tmp == -1)
                break;
            System.arraycopy(buf,0,contents,bytes_read,tmp);
            bytes_read+=tmp;
        }

        byte[] retval=new byte[bytes_read];
        System.arraycopy(contents,0,retval,0,bytes_read);
        return retval;
    }

    /** Returns whitespace-separated strings from the input stream, or null if the end of the stream has been reached */
    public static String readToken(InputStream in) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        int ch;
        while(true) {
            try {
                ch=in.read();
                if(ch == -1)
                    return sb.length() > 0? sb.toString() : null;
                if(Character.isWhitespace(ch)) {
                    if(first)
                        continue;
                    break;
                }
                sb.append((char)ch);
                first=false;
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


    public static void writeByteBuffer(byte[] buf,DataOutput out) throws Exception {
        writeByteBuffer(buf,0,buf.length,out);
    }

    public static void writeByteBuffer(byte[] buf,int offset,int length,DataOutput out) throws Exception {
        if(buf != null) {
            out.write(1);
            out.writeInt(length);
            out.write(buf,offset,length);
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
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);

        out.writeBoolean(msg != null);
        if(msg != null)
            msg.writeTo(out);
        return out.getBuffer();
    }

    public static Message byteBufferToMessage(byte[] buffer,int offset,int length) throws Exception {
        DataInput in=new ByteArrayDataInputStream(buffer,offset,length);
        if(!in.readBoolean())
            return null;
        Message msg=new Message(false); // don't create headers, readFrom() will do this
        msg.readFrom(in);
        return msg;
    }


    public static <T> boolean match(T obj1,T obj2) {
        if(obj1 == null && obj2 == null)
            return true;
        if(obj1 != null)
            return obj1.equals(obj2);
        else
            return obj2.equals(obj1);
    }


    public static boolean match(long[] a1,long[] a2) {
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


    public static boolean patternMatch(String pattern,String str) {
        Pattern pat=Pattern.compile(pattern);
        Matcher matcher=pat.matcher(str);
        return matcher.matches();
    }

    public static boolean productGreaterThan(long n1, long n2, long val) {
        long div=(long)Math.floor(val/(double)n1);
        return div < n2;
    }

    public static <T> boolean different(T one,T two) {
        return !match(one,two);
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


    public static void sleep(long timeout,int nanos) {
        //the Thread.sleep method is not precise at all regarding nanos
        if(timeout > 0 || nanos > 900000) {
            try {
                Thread.sleep(timeout + (nanos / 1000000),(nanos % 1000000));
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        else {
            //this isn't a superb metronome either, but allows a granularity
            //with a reasonable precision in the order of 50ths of millisecond
            long initialTime=System.nanoTime() - 200;
            while(System.nanoTime() < initialTime + nanos) ;
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


    public static long micros() {
        return nanoTime() / 1000;
    }


    /** Returns a random value in the range [1 - range] */
    public static long random(long range) {
        return (long)((Math.random() * range) % range) + 1;
    }


    /** Sleeps between floor and ceiling milliseconds, chosen randomly */
    public static void sleepRandom(long floor,long ceiling) {
        if(ceiling - floor <= 0) {
            return;
        }
        long diff=ceiling - floor;
        long r=(int)((Math.random() * 100000) % diff) + floor;
        sleep(r);
    }


    /**
     * Tosses a coin weighted with probability and returns true or false. Example: if probability=0.8,
     * chances are that in 80% of all cases, true will be returned and false in 20%.
     */
    public static boolean tossWeightedCoin(double probability) {
        if(probability >= 1)
            return true;
        if(probability <= 0)
            return false;
        long r=random(100);
        long cutoff=(long)(probability * 100);
        return r < cutoff;
    }


    public static String dumpThreads() {
        StringBuilder sb=new StringBuilder();
        ThreadMXBean bean=ManagementFactory.getThreadMXBean();
        long[] ids=bean.getAllThreadIds();
        _printThreads(bean,ids,sb);
        long[] deadlocks=bean.findDeadlockedThreads();
        if(deadlocks != null && deadlocks.length > 0) {
            sb.append("deadlocked threads:\n");
            _printThreads(bean,deadlocks,sb);
        }

        deadlocks=bean.findMonitorDeadlockedThreads();
        if(deadlocks != null && deadlocks.length > 0) {
            sb.append("monitor deadlocked threads:\n");
            _printThreads(bean,deadlocks,sb);
        }
        return sb.toString();
    }


    protected static void _printThreads(ThreadMXBean bean,long[] ids,StringBuilder sb) {
        ThreadInfo[] threads=bean.getThreadInfo(ids,20);
        for(ThreadInfo info : threads) {
            if(info == null)
                continue;
            sb.append(info.getThreadName()).append(":\n");
            StackTraceElement[] stack_trace=info.getStackTrace();
            for(StackTraceElement el : stack_trace) {
                sb.append("    at ").append(el.getClassName()).append(".").append(el.getMethodName());
                sb.append("(").append(el.getFileName()).append(":").append(el.getLineNumber()).append(")");
                sb.append("\n");
            }
            sb.append("\n\n");
        }
    }


    public static boolean interruptAndWaitToDie(Thread t) {
        return interruptAndWaitToDie(t,Global.THREAD_SHUTDOWN_WAIT_TIME);
    }

    public static boolean interruptAndWaitToDie(Thread t,long timeout) {
        if(t == null)
            throw new IllegalArgumentException("Thread can not be null");
        t.interrupt(); // interrupts the sleep()
        try {
            t.join(timeout);
        }
        catch(InterruptedException e) {
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
        Collection values=q.values();
        if(values.isEmpty()) {
            sb.append("empty");
        }
        else {
            for(Object o : values) {
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
                        Map<Short,Header> headers=new HashMap<>(m.getHeaders());
                        for(Map.Entry<Short,Header> entry : headers.entrySet()) {
                            short id=entry.getKey();
                            Header value=entry.getValue();
                            String headerToString=null;
                            if(value instanceof FD.FdHeader) {
                                headerToString=value.toString();
                            }
                            else if(value instanceof PingHeader) {
                                headerToString=ClassConfigurator.getProtocol(id) + "-";
                                if(((PingHeader)value).type() == PingHeader.GET_MBRS_REQ) {
                                    headerToString+="GMREQ";
                                }
                                else if(((PingHeader)value).type() == PingHeader.GET_MBRS_RSP) {
                                    headerToString+="GMRSP";
                                }
                                else {
                                    headerToString+="UNKNOWN";
                                }
                            }
                            else {
                                headerToString=ClassConfigurator.getProtocol(id) + "-" + (value == null? "null" : value.toString());
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


    public static String mapToString(Map<? extends Object,? extends Object> map) {
        if(map == null)
            return "null";
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<? extends Object,? extends Object> entry : map.entrySet()) {
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


    public static String printNanos(double time_ns) {
        double us=time_ns / 1_000.0;
        return String.format("%.2f ns (%.2f us)", time_ns, us);
    }

    public static String printTime(long time,TimeUnit unit) {
        long ns=TimeUnit.NANOSECONDS.convert(time,unit);
        long us=TimeUnit.MICROSECONDS.convert(time,unit);
        long ms=TimeUnit.MILLISECONDS.convert(time,unit);
        long secs=TimeUnit.SECONDS.convert(time,unit);

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

        int index=-1;
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

        String str=index != -1? input.substring(0,index) : input;
        return new Tuple<>(str,factor);
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


    public static String[] components(String path,String separator) {
        if(path == null || path.isEmpty())
            return null;
        String[] tmp=path.split(separator + "+"); // multiple separators could be present
        if(tmp == null)
            return null;
        if(tmp.length == 0)
            return null;

        if(tmp[0].isEmpty())
            tmp[0]=separator;
        return tmp;
    }

    /**
     * Fragments a byte buffer into smaller fragments of (max.) frag_size.
     * Example: a byte buffer of 1024 bytes and a frag_size of 248 gives 4 fragments
     * of 248 bytes each and 1 fragment of 32 bytes.
     * @return An array of byte buffers (<code>byte[]</code>).
     */
    public static byte[][] fragmentBuffer(byte[] buf,int frag_size,final int length) {
        byte[] retval[];
        int accumulated_size=0;
        byte[] fragment;
        int tmp_size=0;
        int num_frags;
        int index=0;

        num_frags=length % frag_size == 0? length / frag_size : length / frag_size + 1;
        retval=new byte[num_frags][];

        while(accumulated_size < length) {
            if(accumulated_size + frag_size <= length)
                tmp_size=frag_size;
            else
                tmp_size=length - accumulated_size;
            fragment=new byte[tmp_size];
            System.arraycopy(buf,accumulated_size,fragment,0,tmp_size);
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
    public static List<Range> computeFragOffsets(int offset,int length,int frag_size) {
        List<Range> retval=new ArrayList<>();
        long total_size=length + offset;
        int index=offset;
        int tmp_size=0;
        Range r;

        while(index < total_size) {
            if(index + frag_size <= total_size)
                tmp_size=frag_size;
            else
                tmp_size=(int)(total_size - index);
            r=new Range(index,tmp_size);
            retval.add(r);
            index+=tmp_size;
        }
        return retval;
    }

    public static List<Range> computeFragOffsets(byte[] buf,int frag_size) {
        return computeFragOffsets(0,buf.length,frag_size);
    }

    /**
     * Concatenates smaller fragments into entire buffers.
     * @param fragments An array of byte buffers (<code>byte[]</code>)
     * @return A byte buffer
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
            System.arraycopy(fragments[i],0,ret,index,fragments[i].length);
            index+=fragments[i].length;
        }
        return ret;
    }


    /** Returns true if all elements in the collection are the same */
    public static <T> boolean allEqual(Collection<T> elements) {
        if(elements.isEmpty())
            return false;
        Iterator<T> it=elements.iterator();
        T first=it.next();
        while(it.hasNext()) {
            T el=it.next();
            if(!el.equals(first))
                return false;
        }
        return true;
    }


    public static <T> String printListWithDelimiter(Collection<T> list,String delimiter) {
        return printListWithDelimiter(list,delimiter,MAX_LIST_PRINT_SIZE,true);
    }

    public static <T> String printListWithDelimiter(Collection<T> list,String delimiter,int limit) {
        return printListWithDelimiter(list,delimiter,limit,true);
    }


    public static <T> String printListWithDelimiter(Collection<T> list,String delimiter,int limit,boolean print_size) {
        boolean first=true;
        int count=0, size=print_size? list.size() : 0;
        StringBuilder sb=new StringBuilder(print_size? "(" + size + ") " : "");
        for(T el : list) {
            if(first)
                first=false;
            else
                sb.append(delimiter);
            sb.append(el);
            if(limit > 0 && ++count >= limit) {
                if(size > count)
                    sb.append(" ..."); // .append(list.size()).append("...");
                break;
            }
        }
        return sb.toString();
    }

    public static <T> String printListWithDelimiter(T[] list,String delimiter,int limit) {
        boolean first=true;
        StringBuilder sb=new StringBuilder();
        int count=0, size=list.length;
        for(T el : list) {
            if(first)
                first=false;
            else
                sb.append(delimiter);
            sb.append(el);
            if(limit > 0 && ++count >= limit) {
                if(size > count)
                    sb.append(" ..."); // .append(list.length).append("...");
                break;
            }
        }
        return sb.toString();
    }


    public static <T> String printMapWithDelimiter(Map<T,T> map,String delimiter) {
        boolean first=true;
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<T,T> entry : map.entrySet()) {
            if(first)
                first=false;
            else
                sb.append(delimiter);
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }


    /** Returns true if all elements of c match obj */
    public static boolean all(Collection c,Object obj) {
        for(Iterator iterator=c.iterator(); iterator.hasNext(); ) {
            Object o=iterator.next();
            if(!o.equals(obj))
                return false;
        }
        return true;
    }


    public static List<Address> leftMembers(Collection<Address> old_list,Collection<Address> new_list) {
        if(old_list == null || new_list == null)
            return null;
        List<Address> retval=new ArrayList<>(old_list);
        retval.removeAll(new_list);
        return retval;
    }

    public static List<Address> newMembers(List<Address> old_list,List<Address> new_list) {
        if(old_list == null || new_list == null)
            return null;
        List<Address> retval=new ArrayList<>(new_list);
        retval.removeAll(old_list);
        return retval;
    }

    public static <T> List<T> newElements(List<T> old_list,List<T> new_list) {
        if(new_list == null)
            return new ArrayList<>();
        List<T> retval=new ArrayList<>(new_list);
        if(old_list != null)
            retval.removeAll(old_list);
        return retval;
    }


    /**
     * Selects a random subset of members according to subset_percentage and returns them.
     * Picks no member twice from the same membership. If the percentage is smaller than 1 -> picks 1 member.
     */
    public static List<Address> pickSubset(List<Address> members,double subset_percentage) {
        List<Address> ret=new ArrayList<>(), tmp_mbrs;
        int num_mbrs=members.size(), subset_size, index;

        if(num_mbrs == 0) return ret;
        subset_size=(int)Math.ceil(num_mbrs * subset_percentage);

        tmp_mbrs=new ArrayList<>(members);

        for(int i=subset_size; i > 0 && !tmp_mbrs.isEmpty(); i--) {
            index=(int)((Math.random() * num_mbrs) % tmp_mbrs.size());
            ret.add(tmp_mbrs.get(index));
            tmp_mbrs.remove(index);
        }

        return ret;
    }


    public static <T> boolean contains(T key,T[] list) {
        if(list == null) return false;
        for(T tmp : list)
            if(tmp == key || tmp.equals(key))
                return true;
        return false;
    }


    public static boolean containsViewId(Collection<View> views,ViewId vid) {
        for(View view : views) {
            ViewId tmp=view.getViewId();
            if(tmp.equals(vid))
                return true;
        }
        return false;
    }

    public static boolean containsId(short id,short[] ids) {
        if(ids == null)
            return false;
        for(short tmp : ids)
            if(tmp == id)
                return true;
        return false;
    }


    public static List<View> detectDifferentViews(Map<Address,View> map) {
        final List<View> ret=new ArrayList<>();
        for(View view : map.values()) {
            if(view == null)
                continue;
            ViewId vid=view.getViewId();
            if(!Util.containsViewId(ret,vid))
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
        Set<Address> coords=new HashSet<>();
        Set<Address> all_addrs=new HashSet<>();

        if(map == null)
            return Collections.emptyList();

        for(View view : map.values())
            all_addrs.addAll(view.getMembers());

        for(View view : map.values()) {
            Address coord=view.getCreator();
            if(coord != null)
                coords.add(coord);
        }

        for(Address coord : coords) {
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
        Set<Address> retval=new HashSet<>();
        if(map != null) {
            for(View view : map.values()) {
                Address coord=view.getCreator();
                if(coord != null)
                    retval.add(coord);
            }
        }
        return retval;
    }

    /**
     * Similar to {@link #determineMergeCoords(java.util.Map)} but only actual coordinators are counted: an actual
     * coord is when the sender of a view is the first member of that view
     * @param map
     * @return
     */
    public static Collection<Address> determineActualMergeCoords(Map<Address,View> map) {
        Set<Address> retval=new HashSet<>();
        if(map != null) {
            for(Map.Entry<Address,View> entry : map.entrySet()) {
                Address sender=entry.getKey();
                List<Address> members=entry.getValue().getMembers();
                Address actual_coord=members.isEmpty()? null : members.get(0);
                if(sender.equals(actual_coord))
                    retval.add(sender);
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
    public static int getRank(View view,Address addr) {
        if(view == null || addr == null)
            return 0;
        List<Address> members=view.getMembers();
        for(int i=0; i < members.size(); i++) {
            Address mbr=members.get(i);
            if(mbr.equals(addr))
                return i + 1;
        }
        return 0;
    }

    public static int getRank(Collection<Address> members,Address addr) {
        if(members == null || addr == null)
            return -1;
        int index=0;
        for(Iterator<Address> it=members.iterator(); it.hasNext(); ) {
            Address mbr=it.next();
            if(mbr.equals(addr))
                return index + 1;
            index++;
        }
        return -1;
    }


    public static <T> T pickRandomElement(List<T> list) {
        if(list == null) return null;
        int size=list.size();
        int index=(int)((Math.random() * size * 10) % size);
        return list.get(index);
    }

    public static <T> T pickRandomElement(T[] array) {
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
    public static <T> T pickNext(List<T> list,T obj) {
        if(list == null || obj == null)
            return null;
        Object[] array=list.toArray();
        for(int i=0; i < array.length; i++) {
            T tmp=(T)array[i];
            if(tmp != null && tmp.equals(obj))
                return (T)array[(i + 1) % array.length];
        }
        return null;
    }

    /** Returns the next min(N,list.size()) elements after obj */
    public static <T> List<T> pickNext(List<T> list,T obj,int num) {
        List<T> retval=new ArrayList<>();
        if(list == null || list.size() < 2)
            return retval;
        int index=list.indexOf(obj);
        if(index != -1) {
            for(int i=1; i <= num && i < list.size(); i++) {
                T tmp=list.get((index + i) % list.size());
                if(!retval.contains(tmp))
                    retval.add(tmp);
            }
        }
        return retval;
    }


    public static JChannel createChannel(Protocol... prots) throws Exception {
        JChannel ch=new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        ch.setProtocolStack(stack);
        for(Protocol prot : prots) {
            stack.addProtocol(prot);
            prot.setProtocolStack(stack);
        }
        stack.init();
        return ch;
    }

    public static byte[] generateArray(int size) {
        byte[] retval=new byte[size];
        for(int i=0; i < retval.length; i++) {
            byte b=(byte)Util.random(26);
            retval[i]=b;
        }
        return retval;
    }


    public static Address createRandomAddress() {
        return createRandomAddress(generateLocalName());
    }

    /** Returns an array of num random addresses, named A, B, C etc */
    public static Address[] createRandomAddresses(int num) {
        return createRandomAddresses(num, false);
    }

    public static Address[] createRandomAddresses(int num,boolean use_numbers) {
        Address[] addresses=new Address[num];
        int number=1;
        char c='A';
        for(int i=0; i < addresses.length; i++)
            addresses[i]=Util.createRandomAddress(use_numbers? String.valueOf(number++) : String.valueOf(c++));
        return addresses;
    }

    public static Address createRandomAddress(String name) {
        UUID retval=UUID.randomUUID();
        UUID.add(retval,name);
        return retval;
    }

    public static Object[][] createTimer() {
        return new Object[][]{
          {new DefaultTimeScheduler(5)},
          {new TimeScheduler2()},
          {new TimeScheduler3()},
          {new HashedTimingWheel(5)}
        };
    }

    /**
     * Returns all members that left between 2 views. All members that are element of old_mbrs but not element of
     * new_mbrs are returned.
     */
    public static List<Address> determineLeftMembers(List<Address> old_mbrs,List<Address> new_mbrs) {
        List<Address> retval=new ArrayList<>();
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
        for(View view : views) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(view.getViewId());
        }
        return sb.toString();
    }

    public static <T> String print(Collection<T> objs) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(T obj : objs) {
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
        for(Map.Entry<T,T> entry : map.entrySet()) {
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
            for(PingData rsp : rsps) {
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


    public static String print(ByteBuffer buf) {
        return buf == null? "null" : String.format("[pos=%d lim=%d cap=%d]", buf.position(), buf.limit(), buf.capacity());
    }


    /**
     * Tries to load the class from the current thread's context class loader. If
     * not successful, tries to load the class from the current instance.
     * @param classname Desired class.
     * @param clazz     Class object used to obtain a class loader
     *                  if no context class loader is available.
     * @return Class, or null on failure.
     */
    public static Class loadClass(String classname,Class clazz) throws ClassNotFoundException {
        return loadClass(classname, clazz != null? clazz.getClassLoader() : null);
    }

    /**
     * Tries to load the class from the preferred loader.  If not successful, tries to 
     * load the class from the current thread's context class loader or system class loader.
     * @param classname Desired class name.
     * @param preferredLoader The preferred class loader
     * @return the loaded class.
     * @throws ClassNotFoundException if the class could not be loaded by any loader
     */
    public static Class<?> loadClass(String classname, ClassLoader preferredLoader) throws ClassNotFoundException {
        ClassNotFoundException exception = null;
        List<ClassLoader> list=preferredLoader != null?
          Arrays.asList(preferredLoader, Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader()) :
          Arrays.asList(Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader());
        for(ClassLoader loader: list) {
            try {
                return loader.loadClass(classname);
            }
            catch (ClassNotFoundException e) {
                if(exception == null)
                    exception=e;
            }
        }
        throw exception;
    }

    public static Field[] getAllDeclaredFields(final Class clazz) {
        return getAllDeclaredFieldsWithAnnotations(clazz);
    }

    public static Field[] getAllDeclaredFieldsWithAnnotations(final Class clazz,Class<? extends Annotation>... annotations) {
        List<Field> list=new ArrayList<>(30);
        for(Class curr=clazz; curr != null; curr=curr.getSuperclass()) {
            Field[] fields=curr.getDeclaredFields();
            if(fields != null) {
                for(Field field : fields) {
                    if(annotations != null && annotations.length > 0) {
                        for(Class<? extends Annotation> annotation : annotations) {
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

    public static Method[] getAllDeclaredMethodsWithAnnotations(final Class clazz,Class<? extends Annotation>... annotations) {
        List<Method> list=new ArrayList<>(30);
        for(Class curr=clazz; curr != null; curr=curr.getSuperclass()) {
            Method[] methods=curr.getDeclaredMethods();
            if(methods != null) {
                for(Method method : methods) {
                    if(annotations != null && annotations.length > 0) {
                        for(Class<? extends Annotation> annotation : annotations) {
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


    public static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annotationClass) {
        for(Class<?> curr=clazz; curr != null; curr=curr.getSuperclass()) {
            A ann=curr.getAnnotation(annotationClass);
            if(ann != null)
                return ann;
        }
        return null;
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

    public static void setField(Field field,Object target,Object value) {
        if(!Modifier.isPublic(field.getModifiers())) {
            field.setAccessible(true);
        }
        try {
            field.set(target,value);
        }
        catch(IllegalAccessException iae) {
            throw new IllegalArgumentException("Could not set field " + field,iae);
        }
    }

    public static Object getField(Field field,Object target) {
        if(!Modifier.isPublic(field.getModifiers())) {
            field.setAccessible(true);
        }
        try {
            return field.get(target);
        }
        catch(IllegalAccessException iae) {
            throw new IllegalArgumentException("Could not get field " + field,iae);
        }
    }


    public static Field findField(Object target,List<String> possible_names) {
        if(target == null)
            return null;
        for(Class<?> clazz=target.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            for(String name : possible_names) {
                try {
                    return clazz.getDeclaredField(name);
                }
                catch(Exception e) {
                }
            }
        }
        return null;
    }


    public static Method findMethod(Object target,List<String> possible_names,Class<?>... parameter_types) {
        if(target == null)
            return null;
        return findMethod(target.getClass(),possible_names,parameter_types);
    }


    public static Method findMethod(Class<?> root_class,List<String> possible_names,Class<?>... parameter_types) {
        for(Class<?> clazz=root_class; clazz != null; clazz=clazz.getSuperclass()) {
            for(String name : possible_names) {
                try {
                    return clazz.getDeclaredMethod(name,parameter_types);
                }
                catch(Exception e) {
                }
            }
        }
        return null;
    }


    public static <T> Set<Class<T>> findClassesAssignableFrom(String packageName,Class<T> assignableFrom)
      throws IOException, ClassNotFoundException {
        ClassLoader loader=Thread.currentThread().getContextClassLoader();
        Set<Class<T>> classes=new HashSet<>();
        String path=packageName.replace('.','/');
        URL resource=loader.getResource(path);
        if(resource != null) {
            String filePath=resource.getFile();
            if(filePath != null && new File(filePath).isDirectory()) {
                for(String file : new File(filePath).list()) {
                    if(file.endsWith(".class")) {
                        String name=packageName + '.' + file.substring(0,file.indexOf(".class"));
                        Class<T> clazz=(Class<T>)Class.forName(name);
                        if(assignableFrom.isAssignableFrom(clazz))
                            classes.add(clazz);
                    }
                }
            }
        }
        return classes;
    }

    public static List<Class<?>> findClassesAnnotatedWith(String packageName,Class<? extends Annotation> a) throws IOException, ClassNotFoundException {
        List<Class<?>> classes=new ArrayList<>();
        recurse(classes,packageName,a);
        return classes;
    }

    private static void recurse(List<Class<?>> classes,String packageName,Class<? extends Annotation> a) throws ClassNotFoundException {
        ClassLoader loader=Thread.currentThread().getContextClassLoader();
        String path=packageName.replace('.','/');
        URL resource=loader.getResource(path);
        if(resource != null) {
            String filePath=resource.getFile();
            if(filePath != null && new File(filePath).isDirectory()) {
                for(String file : new File(filePath).list()) {
                    if(file.endsWith(".class")) {
                        String name=packageName + '.' + file.substring(0,file.indexOf(".class"));
                        Class<?> clazz=Class.forName(name);
                        if(clazz.isAnnotationPresent(a))
                            classes.add(clazz);
                    }
                    else if(new File(filePath,file).isDirectory()) {
                        recurse(classes,packageName + "." + file,a);
                    }
                }
            }
        }
    }


    public static InputStream getResourceAsStream(String name,Class clazz) {
        ClassLoader loader;
        InputStream retval=null;

        // https://issues.jboss.org/browse/JGRP-1762: load the classloader from the defining class first
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
            loader=Thread.currentThread().getContextClassLoader();
            if(loader != null) {
                retval=loader.getResourceAsStream(name);
                if(retval != null)
                    return retval;
            }
        }
        catch(Throwable t) {
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
    public static boolean sameHost(Address one,Address two) {
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
        if(rejection_policy.toLowerCase().startsWith(CustomRejectionPolicy.NAME))
            return new CustomRejectionPolicy(rejection_policy);
        if(rejection_policy.toLowerCase().startsWith(ProgressCheckRejectionPolicy.NAME))
            return new ProgressCheckRejectionPolicy(rejection_policy);
        throw new IllegalArgumentException("rejection policy \"" + rejection_policy + "\" not known");
    }


    /**
     * Parses comma-delimited longs; e.g., 2000,4000,8000.
     * Returns array of long, or null.
     */
    public static int[] parseCommaDelimitedInts(String s) {
        StringTokenizer tok;
        List<Integer> v=new ArrayList<>();
        Integer l;
        int[] retval=null;

        if(s == null) return null;
        tok=new StringTokenizer(s,",");
        while(tok.hasMoreTokens()) {
            l=new Integer(tok.nextToken());
            v.add(l);
        }
        if(v.isEmpty()) return null;
        retval=new int[v.size()];
        for(int i=0; i < v.size(); i++)
            retval[i]=v.get(i);
        return retval;
    }

    /**
     * Parses comma-delimited longs; e.g., 2000,4000,8000.
     * Returns array of long, or null.
     */
    public static long[] parseCommaDelimitedLongs(String s) {
        StringTokenizer tok;
        List<Long> v=new ArrayList<>();
        Long l;
        long[] retval=null;

        if(s == null) return null;
        tok=new StringTokenizer(s,",");
        while(tok.hasMoreTokens()) {
            l=new Long(tok.nextToken());
            v.add(l);
        }
        if(v.isEmpty()) return null;
        retval=new long[v.size()];
        for(int i=0; i < v.size(); i++)
            retval[i]=v.get(i);
        return retval;
    }

    /** e.g. "bela,jeannette,michelle" --> List{"bela", "jeannette", "michelle"} */
    public static List<String> parseCommaDelimitedStrings(String l) {
        return parseStringList(l,",");
    }

    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Returns a list of IpAddresses
     */
    public static List<PhysicalAddress> parseCommaDelimitedHosts(String hosts,int port_range) throws UnknownHostException {
        StringTokenizer tok=hosts != null? new StringTokenizer(hosts,",") : null;
        String t;
        IpAddress addr;
        Set<PhysicalAddress> retval=new HashSet<>();

        while(tok != null && tok.hasMoreTokens()) {
            t=tok.nextToken().trim();
            String host=t.substring(0,t.indexOf('['));
            host=host.trim();
            int port=Integer.parseInt(t.substring(t.indexOf('[') + 1,t.indexOf(']')));
            for(int i=port; i <= port + port_range; i++) {
                addr=new IpAddress(host,i);
                retval.add(addr);
            }
        }
        return new LinkedList<>(retval);
    }


    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of
     * InetSocketAddress
     */
    public static List<InetSocketAddress> parseCommaDelimitedHosts2(String hosts,int port_range) {

        StringTokenizer tok=new StringTokenizer(hosts,",");
        String t;
        InetSocketAddress addr;
        Set<InetSocketAddress> retval=new HashSet<>();

        while(tok.hasMoreTokens()) {
            t=tok.nextToken().trim();
            String host=t.substring(0,t.indexOf('['));
            host=host.trim();
            int port=Integer.parseInt(t.substring(t.indexOf('[') + 1,t.indexOf(']')));
            for(int i=port; i < port + port_range; i++) {
                addr=new InetSocketAddress(host,i);
                retval.add(addr);
            }
        }
        return new LinkedList<>(retval);
    }

    public static List<String> parseStringList(String l,String separator) {
        List<String> tmp=new LinkedList<>();
        StringTokenizer tok=new StringTokenizer(l,separator);
        String t;

        while(tok.hasMoreTokens()) {
            t=tok.nextToken();
            tmp.add(t.trim());
        }

        return tmp;
    }

    public static Map<String,String> parseCommaDelimitedProps(String s) {
        if (s == null)
            return null;
        Map<String,String> props=new HashMap<>();
        Pattern p=Pattern.compile("\\s*([^=\\s]+)\\s*=\\s*([^=\\s,]+)\\s*,?"); //Pattern.compile("\\s*([^=\\s]+)\\s*=\\s([^=\\s]+)\\s*,?");
        Matcher matcher=p.matcher(s);
        while(matcher.find()) {
            props.put(matcher.group(1), matcher.group(2));
        }
        return props;
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
     * @return A String containing the contents of the line, not including
     * any line-termination characters, or null if the end of the
     * stream has been reached
     * @throws IOException If an I/O error occurs
     */
    public static String readLine(InputStream in) throws IOException {
        StringBuilder sb=new StringBuilder(35);
        int ch;

        while(true) {
            ch=in.read();
            if(ch == -1)
                return null;
            if(ch == '\r')
                ;
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


    /**
     * @param s
     * @return List<NetworkInterface>
     */
    public static List<NetworkInterface> parseInterfaceList(String s) throws Exception {
        List<NetworkInterface> interfaces=new ArrayList<>(10);
        if(s == null)
            return null;

        StringTokenizer tok=new StringTokenizer(s,",");
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

        for(NetworkInterface intf : interfaces) {
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
            return hostname.substring(0,index);
        else
            return hostname;
    }

    /**
     * Performs the flush of the given channel for the specified flush participants and the given
     * number of attempts along with random sleep time after each such attempt.
     * @param c                         the channel
     * @param flushParticipants         the flush participants in this flush attempt
     * @param numberOfAttempts          the number of flush attempts
     * @param randomSleepTimeoutFloor   the minimum sleep time between attempts in ms
     * @param randomSleepTimeoutCeiling the maximum sleep time between attempts in ms
     * @return true if channel was flushed successfully, false otherwise
     * @see Channel#startFlush(List,boolean)
     */
    public static boolean startFlush(Channel c,List<Address> flushParticipants,
                                     int numberOfAttempts,long randomSleepTimeoutFloor,long randomSleepTimeoutCeiling) {
        int attemptCount=0;
        while(attemptCount < numberOfAttempts) {
            try {
                c.startFlush(flushParticipants,false);
                return true;
            }
            catch(Exception e) {
                Util.sleepRandom(randomSleepTimeoutFloor,randomSleepTimeoutCeiling);
                attemptCount++;
            }
        }
        return false;
    }

    /**
     * Performs the flush of the given channel and the specified flush participants
     * @param c                 the channel
     * @param flushParticipants the flush participants in this flush attempt
     * @see Channel#startFlush(List,boolean)
     */
    public static boolean startFlush(Channel c,List<Address> flushParticipants) {
        return startFlush(c,flushParticipants,4,1000,5000);
    }

    /**
     * Performs the flush of the given channel within the specfied number of attempts along with random
     * sleep time after each such attempt.
     * @param c                         the channel
     * @param numberOfAttempts          the number of flush attempts
     * @param randomSleepTimeoutFloor   the minimum sleep time between attempts in ms
     * @param randomSleepTimeoutCeiling the maximum sleep time between attempts in ms
     * @return true if channel was flushed successfully, false otherwise
     * @see Channel#startFlush(boolean)
     */
    public static boolean startFlush(Channel c,int numberOfAttempts,long randomSleepTimeoutFloor,long randomSleepTimeoutCeiling) {
        int attemptCount=0;
        while(attemptCount < numberOfAttempts) {
            try {
                c.startFlush(false);
                return true;
            }
            catch(Exception e) {
                Util.sleepRandom(randomSleepTimeoutFloor,randomSleepTimeoutCeiling);
                attemptCount++;
            }
        }
        return false;
    }

    /**
     * Performs the flush of the given channel
     * @param c the channel
     * @return true if channel was flushed successfully, false otherwise
     * @see Channel#startFlush(boolean)
     */
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
        catch(Throwable e) {
        }
        if(retval == null) {
            try {
                retval=shortName(InetAddress.getByName(null).getHostName());
            }
            catch(Throwable e) {
                retval="localhost";
            }
        }

        long counter=Util.random(Short.MAX_VALUE * 2);
        return retval + "-" + counter;
    }


    public static <K,V> ConcurrentMap<K,V> createConcurrentMap(int initial_capacity,float load_factor,int concurrency_level) {
        return new ConcurrentHashMap<>(initial_capacity,load_factor,concurrency_level);
    }

    public static <K,V> ConcurrentMap<K,V> createConcurrentMap(int initial_capacity) {
        return new ConcurrentHashMap<>(initial_capacity);
    }

    public static <K,V> ConcurrentMap<K,V> createConcurrentMap() {
        return new ConcurrentHashMap<>(CCHM_INITIAL_CAPACITY,CCHM_LOAD_FACTOR,CCHM_CONCURRENCY_LEVEL);
    }

    public static ServerSocket createServerSocket(SocketFactory factory, String service_name, InetAddress bind_addr, int start_port) {
        ServerSocket ret=null;

        while(true) {
            try {
                ret=factory.createServerSocket(service_name,start_port,50,bind_addr);
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


    public static ServerSocket createServerSocket(SocketFactory factory,String service_name,InetAddress bind_addr,
                                                  int start_port,int end_port) throws Exception {
        int original_start_port=start_port;
        ServerSocket srv_sock=null;

        while(true) {
            try {
                if(srv_sock != null);
                    Util.close(srv_sock);
                srv_sock=factory.createServerSocket(service_name);
                InetSocketAddress sock_addr=new InetSocketAddress(bind_addr, start_port);
                srv_sock.bind(sock_addr);
                return srv_sock;
            }
            catch(SocketException bind_ex) {
                if(start_port == end_port)
                    throw new BindException(String.format("no port available in range [%d .. %d] (bind_addr=%s)",
                                                          original_start_port, end_port, bind_addr));
                if(bind_addr != null && !bind_addr.isLoopbackAddress()) {
                    NetworkInterface nic=NetworkInterface.getByInetAddress(bind_addr);
                    if(nic == null)
                        throw new BindException("bind_addr " + bind_addr + " is not a valid interface: " + bind_ex);
                }
                start_port++;
            }
        }
    }

    /**
     * Finds first available port starting at start_port and returns server
     * socket. Will not bind to port >end_port. Sets srv_port
     */
    public static ServerSocket createServerSocketAndBind(SocketFactory factory,String service_name,InetAddress bind_addr,
                                                  int start_port,int end_port) throws Exception {
        ServerSocket ret=factory.createServerSocket(service_name);
        bind(ret, bind_addr, start_port, end_port);
        return ret;
    }

    public static void bind(ServerSocket srv_sock, InetAddress bind_addr,
                            int start_port, int end_port) throws Exception {
        bind(srv_sock, bind_addr, start_port, end_port, 50);
    }

    public static void bind(ServerSocket srv_sock, InetAddress bind_addr,
                            int start_port, int end_port, int backlog) throws Exception {
        int original_start_port=start_port;

        while(true) {
            try {
                InetSocketAddress sock_addr=new InetSocketAddress(bind_addr, start_port);
                srv_sock.bind(sock_addr, backlog);
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
    }


    public static ServerSocketChannel createServerSocketChannelAndBind(InetAddress bind_addr,
                                                                int start_port, int end_port) throws Exception {
        ServerSocketChannel channel=ServerSocketChannel.open();
        bind(channel, bind_addr, start_port, end_port);
        return channel;
    }

    public static ServerSocketChannel createServerSocketChannel(InetAddress bind_addr,
                                                                int start_port, int end_port) throws Exception {
        int original_start_port=start_port;
        ServerSocketChannel ch=null;
        while(true) {
            try {
                if(ch != null)
                    Util.close(ch);
                ch=ServerSocketChannel.open();
                ch.bind(new InetSocketAddress(bind_addr, start_port), 50);
                return ch;
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
            }
        }
    }


    public static void bind(final ServerSocketChannel ch, InetAddress bind_addr, int start_port, int end_port) throws Exception {
        bind(ch, bind_addr, start_port, end_port, 50);
    }


    public static void bind(final ServerSocketChannel ch, InetAddress bind_addr, int start_port, int end_port, int backlog) throws Exception {
        int original_start_port=start_port;
        while(true) {
            try {
                ch.bind(new InetSocketAddress(bind_addr, start_port), backlog);
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
    }


    /**
     * Creates a DatagramSocket bound to addr. If addr is null, socket won't be bound. If address is already in use,
     * start_port will be incremented until a socket can be created.
     * @param addr The InetAddress to which the socket should be bound. If null, the socket will not be bound.
     * @param port The port which the socket should use. If 0, a random port will be used. If > 0, but port is already
     *             in use, it will be incremented until an unused port is found, or until MAX_PORT is reached.
     */
    public static DatagramSocket createDatagramSocket(SocketFactory factory,String service_name,InetAddress addr,int port) throws Exception {
        DatagramSocket sock=null;

        if(addr == null) {
            if(port == 0) {
                return factory.createDatagramSocket(service_name);
            }
            else {
                while(port < MAX_PORT) {
                    try {
                        return factory.createDatagramSocket(service_name,port);
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
                    return factory.createDatagramSocket(service_name,port,addr);
                }
                catch(BindException bind_ex) { // port already used
                    port++;
                }
            }
        }
        return sock; // will never be reached, but the stupid compiler didn't figure it out...
    }


    public static MulticastSocket createMulticastSocket(SocketFactory factory,String service_name,InetAddress mcast_addr,int port,Log log) throws IOException {
        if(mcast_addr != null && !mcast_addr.isMulticastAddress())
            throw new IllegalArgumentException("mcast_addr (" + mcast_addr + ") is not a valid multicast address");

        SocketAddress saddr=new InetSocketAddress(mcast_addr,port);
        MulticastSocket retval=null;

        try {
            retval=factory.createMulticastSocket(service_name,saddr);
        }
        catch(IOException ex) {
            if(log != null && log.isWarnEnabled()) {
                StringBuilder sb=new StringBuilder();
                String type=mcast_addr != null? mcast_addr instanceof Inet4Address? "IPv4" : "IPv6" : "n/a";
                sb.append("could not bind to ").append(mcast_addr).append(" (").append(type).append(" address)")
                        .append("; make sure your mcast_addr is of the same type as the preferred IP stack (IPv4 or IPv6)")
                        .append(" by checking the value of the system properties java.net.preferIPv4Stack and java.net.preferIPv6Addresses.")
                        .append("\nWill ignore mcast_addr, but this may lead to cross talking " +
                                "(see http://www.jboss.org/community/docs/DOC-9469 for details). ")
                        .append("\nException was: ").append(ex);
                log.warn(sb.toString());
            }
        }
        if(retval == null)
            retval=factory.createMulticastSocket(service_name,port);
        return retval;
    }


    /**
     * Method used by PropertyConverters.BindInterface to check that a bind_address is
     * consistent with a specified interface
     * <p/>
     * Idea:
     * 1. We are passed a bind_addr, which may be null
     * 2. If non-null, check that bind_addr is on bind_interface - if not, throw exception,
     * otherwise, return the original bind_addr
     * 3. If null, get first non-loopback address on bind_interface, using stack preference to
     * get the IP version. If no non-loopback address, then just return null (i.e. the
     * bind_interface did not influence the decision).
     */
    public static InetAddress validateBindAddressFromInterface(InetAddress bind_addr,String bind_interface_str) throws UnknownHostException, SocketException {
        NetworkInterface bind_intf=null;

        if(bind_addr != null && bind_addr.isLoopbackAddress())
            return bind_addr;

        // 1. if bind_interface_str is null, or empty, no constraint on bind_addr
        if(bind_interface_str == null || bind_interface_str.trim().isEmpty())
            return bind_addr;

        // 2. get the preferred IP version for the JVM - it will be IPv4 or IPv6
        StackType ip_version=getIpStackType();

        // 3. if bind_interface_str specified, get interface and check that it has correct version
        bind_intf=Util.getByName(bind_interface_str); // NetworkInterface.getByName(bind_interface_str);
        if(bind_intf != null) {
            // check that the interface supports the IP version
            boolean supportsVersion=interfaceHasIPAddresses(bind_intf,ip_version);
            if(!supportsVersion)
                throw new IllegalArgumentException("bind_interface " + bind_interface_str + " has incorrect IP version");
        }
        else {
            // (bind_intf == null)
            throw new UnknownHostException("network interface " + bind_interface_str + " not found");
        }

        // 3. intf and bind_addr are both are specified, bind_addr needs to be on intf
        if(bind_addr != null) {

            boolean hasAddress=false;

            // get all the InetAddresses defined on the interface
            Enumeration addresses=bind_intf.getInetAddresses();

            while(addresses != null && addresses.hasMoreElements()) {
                // get the next InetAddress for the current interface
                InetAddress address=(InetAddress)addresses.nextElement();

                // check if address is on interface
                if(bind_addr.equals(address)) {
                    hasAddress=true;
                    break;
                }
            }

            if(!hasAddress) {
                String bind_addr_str=bind_addr.getHostAddress();
                throw new IllegalArgumentException("network interface " + bind_interface_str + " does not contain address " + bind_addr_str);
            }

        }
        // 4. if only interface is specified, get first non-loopback address on that interface,
        else {
            bind_addr=getAddress(bind_intf,AddressScope.NON_LOOPBACK);
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

    /** Finds a network interface of sub-interface with the given name */
    public static NetworkInterface getByName(String name) throws SocketException {
        if(name == null) return null;
        Enumeration<NetworkInterface> en=NetworkInterface.getNetworkInterfaces();
        while(en.hasMoreElements()) {
            NetworkInterface intf=en.nextElement();
            if(intf.getName().equals(name))
                return intf;
            Enumeration<NetworkInterface> en2=intf.getSubInterfaces();
            while(en2.hasMoreElements()) {
                NetworkInterface intf2=en2.nextElement();
                if(intf2.getName().equals(name)) {
                    return intf2;
                }
            }
        }
        return null;
    }


    public static boolean checkForLinux() {
        return checkForPresence("os.name","linux");
    }

    public static boolean checkForHp() {
        return checkForPresence("os.name","hp");
    }

    public static boolean checkForSolaris() {
        return checkForPresence("os.name","sun");
    }

    public static boolean checkForWindows() {
        return checkForPresence("os.name","win");
    }

    public static boolean checkForAndroid() {
        return contains("java.vm.vendor", "android");
    }

    public static boolean checkForMac() {return checkForPresence("os.name", "mac");}

    private static boolean checkForPresence(String key,String value) {
        try {
            String tmp=System.getProperty(key);
            return tmp != null && tmp.trim().toLowerCase().startsWith(value);
        }
        catch(Throwable t) {
            return false;
        }
    }

    private static boolean contains(String key,String value) {
        try {
            String tmp=System.getProperty(key);
            return tmp != null && tmp.trim().toLowerCase().contains(value.trim().toLowerCase());
        }
        catch(Throwable t) {
            return false;
        }
    }


    /** IP related utilities */
    public static InetAddress getLocalhost(StackType ip_version) throws UnknownHostException {
        if(ip_version == StackType.IPv4)
            return InetAddress.getByName("127.0.0.1");
        else
            return InetAddress.getByName("::1");
    }

    public static InetAddress getLocalhost() throws UnknownHostException {
        return getLocalhost(Util.getIpStackType());
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
        InetAddress address=null;

        Enumeration intfs=NetworkInterface.getNetworkInterfaces();
        while(intfs.hasMoreElements()) {
            NetworkInterface intf=(NetworkInterface)intfs.nextElement();
            try {
                if(intf.isUp()) {
                    address=getAddress(intf,scope);
                    if(address != null)
                        return address;
                }
            }
            catch(SocketException e) {
            }
        }
        return null;
    }

    /**
     * Returns a valid interface address based on a pattern. Iterates over all interfaces that are up and
     * returns the first match, based on the address or interface name
     * @param pattern Can be "match-addr:<pattern></pattern>" or "match-interface:<pattern></pattern>". Example:<p/>
     *                match-addr:192.168.*
     * @return InetAddress or null if not found
     */
    public static InetAddress getAddressByPatternMatch(String pattern) throws Exception {
        if(pattern == null) return null;
        String real_pattern=null;
        byte type=0; // 1=match-interface, 2: match-addr, 3: match-host,

        if(pattern.startsWith(Global.MATCH_INTF)) {
            type=1;
            real_pattern=pattern.substring(Global.MATCH_INTF.length() + 1);
        }
        else if(pattern.startsWith(Global.MATCH_ADDR)) {
            type=2;
            real_pattern=pattern.substring(Global.MATCH_ADDR.length() + 1);
        }
        else if(pattern.startsWith(Global.MATCH_HOST)) {
            type=3;
            real_pattern=pattern.substring(Global.MATCH_HOST.length() + 1);
        }

        if(real_pattern == null)
            throw new IllegalArgumentException("expected " + Global.MATCH_ADDR + ":<pattern>, " +
                                                 Global.MATCH_HOST + ":<pattern> or " + Global.MATCH_INTF + ":<pattern>");

        Pattern pat=Pattern.compile(real_pattern);
        Enumeration intfs=NetworkInterface.getNetworkInterfaces();
        while(intfs.hasMoreElements()) {
            NetworkInterface intf=(NetworkInterface)intfs.nextElement();
            try {
                if(!intf.isUp())
                    continue;
                switch(type) {
                    case 1: // match by interface name
                        String interface_name=intf.getName();
                        Matcher matcher=pat.matcher(interface_name);
                        if(matcher.matches())
                            return getAddress(intf,null);
                        break;
                    case 2: // match by host address
                    case 3: // match by host name
                        for(Enumeration<InetAddress> en=intf.getInetAddresses(); en.hasMoreElements(); ) {
                            InetAddress addr=en.nextElement();
                            String name=type == 3? addr.getHostName() : addr.getHostAddress();
                            matcher=pat.matcher(name);
                            if(matcher.matches())
                                return addr;
                        }
                        break;
                }
            }
            catch(SocketException e) {
            }
        }
        return null;
    }


    /**
     * Returns the first address on the given interface on the current host, which satisfies scope
     * @param intf the interface to be checked
     */
    public static InetAddress getAddress(NetworkInterface intf,AddressScope scope) {
        StackType ip_version=Util.getIpStackType();
        for(Enumeration addresses=intf.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress addr=(InetAddress)addresses.nextElement();
            boolean match=scope == null;
            if(scope != null) {
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
            }

            if(match) {
                if((addr instanceof Inet4Address && ip_version == StackType.IPv4) ||
                  (addr instanceof Inet6Address && ip_version == StackType.IPv6))
                    return addr;
            }
        }
        return null;
    }


    /**
     * A function to check if an interface supports an IP version (i.e has addresses
     * defined for that IP version).
     * @param intf
     * @return
     */
    public static boolean interfaceHasIPAddresses(NetworkInterface intf,StackType ip_version) throws UnknownHostException {
        boolean supportsVersion=false;
        if(intf != null) {
            // get all the InetAddresses defined on the interface
            Enumeration addresses=intf.getInetAddresses();
            while(addresses != null && addresses.hasMoreElements()) {
                // get the next InetAddress for the current interface
                InetAddress address=(InetAddress)addresses.nextElement();

                // check if we find an address of correct version
                if((address instanceof Inet4Address && (ip_version == StackType.IPv4)) ||
                  (address instanceof Inet6Address && (ip_version == StackType.IPv6))) {
                    supportsVersion=true;
                    break;
                }
            }
        }
        else {
            throw new UnknownHostException("network interface " + intf + " not found");
        }
        return supportsVersion;
    }

    public static StackType getIpStackType() {
        return ip_stack_type;
    }

    /** Returns true if the 2 addresses are of the same type (IPv4 or IPv6) */
    public static boolean sameAddresses(InetAddress one,InetAddress two) {
        return one == null
          || two == null
          || (one instanceof Inet6Address && two instanceof Inet6Address)
          || (one instanceof Inet4Address && two instanceof Inet4Address);
    }

    /**
     * Tries to determine the type of IP stack from the available interfaces and their addresses and from the
     * system properties (java.net.preferIPv4Stack and java.net.preferIPv6Addresses)
     * @return StackType.IPv4 for an IPv4 only stack, StackYTypeIPv6 for an IPv6 only stack, and StackType.Unknown
     * if the type cannot be detected
     */
    private static StackType _getIpStackType() {
        boolean isIPv4StackAvailable=isStackAvailable(true);
        boolean isIPv6StackAvailable=isStackAvailable(false);

        // if only IPv4 stack available
        if(isIPv4StackAvailable && !isIPv6StackAvailable) {
            return StackType.IPv4;
        }
        // if only IPv6 stack available
        else if(isIPv6StackAvailable && !isIPv4StackAvailable) {
            return StackType.IPv6;
        }
        // if dual stack
        else if(isIPv4StackAvailable && isIPv6StackAvailable) {
            // get the System property which records user preference for a stack on a dual stack machine
            if(Boolean.getBoolean(Global.IPv4)) // has preference over java.net.preferIPv6Addresses
                return StackType.IPv4;
            return StackType.IPv6;
        }
        return StackType.Unknown;
    }


    public static boolean isStackAvailable(boolean ipv4) {
        Collection<InetAddress> all_addrs=getAllAvailableAddresses();
        for(InetAddress addr : all_addrs)
            if(ipv4 && addr instanceof Inet4Address || (!ipv4 && addr instanceof Inet6Address))
                return true;
        return false;
    }


    public static List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
        List<NetworkInterface> retval=new ArrayList<>(10);
        NetworkInterface intf;
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
            intf=(NetworkInterface)en.nextElement();
            retval.add(intf);
        }
        return retval;
    }

    public static Collection<InetAddress> getAllAvailableAddresses() {
        Set<InetAddress> retval=new HashSet<>();
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

    public static void checkIfValidAddress(InetAddress bind_addr,String prot_name) throws Exception {
        // N.B. bind_addr.isAnyLocalAddress() is not OK
        if (bind_addr.isLoopbackAddress())
            return;
        Collection<InetAddress> addrs=getAllAvailableAddresses();
        for(InetAddress addr : addrs) {
            if(addr.equals(bind_addr))
                return;
        }
        throw new BindException("[" + prot_name + "] " + bind_addr + " is not a valid address on any local network interface");
    }


    /**
     * Returns a value associated wither with one or more system properties, or found in the props map
     * @param system_props
     * @param props         List of properties read from the configuration file
     * @param prop_name     The name of the property, will be removed from props if found
     * @param default_value Used to return a default value if the properties or system properties didn't have the value
     * @return The value, or null if not found
     */
    public static String getProperty(String[] system_props,Properties props,String prop_name,String default_value) {
        String retval=null;
        if(props != null && prop_name != null) {
            retval=props.getProperty(prop_name);
            props.remove(prop_name);
        }

        String tmp, prop;
        if(retval == null && system_props != null) {
            for(int i=0; i < system_props.length; i++) {
                prop=system_props[i];
                if(prop != null) {
                    try {
                        tmp=System.getProperty(prop);
                        if(tmp != null)
                            return tmp;
                    }
                    catch(SecurityException ex) {
                    }
                }
            }
        }

        if(retval == null)
            return default_value;
        return retval;
    }


    public static boolean isCoordinator(JChannel ch) {
        return isCoordinator(ch.getView(),ch.getAddress());
    }


    public static boolean isCoordinator(View view,Address local_addr) {
        if(view == null || local_addr == null)
            return false;
        List<Address> mbrs=view.getMembers();
        return !(mbrs == null || mbrs.isEmpty()) && local_addr.equals(mbrs.iterator().next());
    }

    public static Address getCoordinator(View view) {
        if(view == null)
            return null;
        List<Address> mbrs=view.getMembers();
        return !mbrs.isEmpty()? mbrs.get(0) : null;
    }

    public static MBeanServer getMBeanServer() {
        List<MBeanServer> servers=MBeanServerFactory.findMBeanServer(null);
        if(servers != null && !servers.isEmpty()) {
            // return 'jboss' server if available
            for(int i=0; i < servers.size(); i++) {
                MBeanServer srv=servers.get(i);
                if("jboss".equalsIgnoreCase(srv.getDefaultDomain()))
                    return srv;
            }

            // return first available server
            return servers.get(0);
        }
        else {
            //if it all fails, create a default
            return MBeanServerFactory.createMBeanServer();
        }
    }


    public static void registerChannel(JChannel channel,String name) {
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


    /**
     * Go through the input string and replace any occurance of ${p} with the
     * props.getProperty(p) value. If there is no such property p defined, then
     * the ${p} reference will remain unchanged.
     * <p/>
     * If the property reference is of the form ${p:v} and there is no such
     * property p, then the default value v will be returned.
     * <p/>
     * If the property reference is of the form ${p1,p2} or ${p1,p2:v} then the
     * primary and the secondary properties will be tried in turn, before
     * returning either the unchanged input, or the default value.
     * <p/>
     * The property ${/} is replaced with System.getProperty("file.separator")
     * value and the property ${:} is replaced with
     * System.getProperty("path.separator").
     * @param string -
     *               the string with possible ${} references
     * @param props  -
     *               the source for ${x} property ref values, null means use
     *               System.getProperty()
     * @return the input string with all property references replaced if any. If
     * there are no valid references the input string will be returned.
     * @throws {@link java.security.AccessControlException}
     *                when not authorised to retrieved system properties
     */
    public static String replaceProperties(final String string,final Properties props) {
        // File separator value
        final String FILE_SEPARATOR=File.separator;

        // Path separator value
        final String PATH_SEPARATOR=File.pathSeparator;

        // File separator alias
        final String FILE_SEPARATOR_ALIAS="/";

        // Path separator alias
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
        for(int i=0; i < chars.length; ++i) {
            char c=chars[i];

            // Dollar sign outside brackets
            if(c == '$' && state != IN_BRACKET)
                state=SEEN_DOLLAR;

                // Open bracket immediatley after dollar
            else if(c == '{' && state == SEEN_DOLLAR) {
                buffer.append(string.substring(start,i - 1));
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

                    String key=string.substring(start + 2,i);

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
                                String realKey=key.substring(0,colon);
                                if(props != null)
                                    value=props.getProperty(realKey);
                                else
                                    value=System.getProperty(realKey);

                                if(value == null) {
                                    // Check for a composite key, "key1,key2"
                                    value=resolveCompositeKey(realKey,props);

                                    // Not a composite key either, use the specified default
                                    if(value == null)
                                        value=key.substring(colon + 1);
                                }
                            }
                            else {
                                // No default, check for a composite key, "key1,key2"
                                value=resolveCompositeKey(key,props);
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
        if(!properties)
            return string;

        // Collect the trailing characters
        if(start != chars.length)
            buffer.append(string.substring(start,chars.length));

        // Done
        return buffer.toString();
    }

    /**
     * Try to resolve a "key" from the provided properties by checking if it is
     * actually a "key1,key2", in which case try first "key1", then "key2". If
     * all fails, return null.
     * <p/>
     * It also accepts "key1," and ",key2".
     * @param key   the key to resolve
     * @param props the properties to use
     * @return the resolved key or null
     */
    private static String resolveCompositeKey(String key,Properties props) {
        String value=null;

        // Look for the comma
        int comma=key.indexOf(',');
        if(comma > -1) {
            // If we have a first part, try resolve it
            if(comma > 0) {
                // Check the first part
                String key1=key.substring(0,comma);
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
        end_index=val.indexOf("}",start_index + 2);
        if(end_index == -1)
            throw new IllegalArgumentException("missing \"}\" in " + val);

        String tmp=getProperty(val.substring(start_index + 2,end_index));
        if(tmp == null)
            return val;
        StringBuilder sb = new StringBuilder();
        sb.append(val.substring(0, start_index))
                .append(tmp).append(val.substring(end_index + 1));
        return sb.toString();
    }

    public static String getProperty(String s) {
        String var, default_val, retval=null;
        int index=s.indexOf(":");
        if(index >= 0) {
            var=s.substring(0,index);
            default_val=s.substring(index + 1);
            if(default_val != null && !default_val.isEmpty())
                default_val=default_val.trim();
            // retval=System.getProperty(var, default_val);
            retval=_getProperty(var,default_val);
        }
        else {
            var=s;
            // retval=System.getProperty(var);
            retval=_getProperty(var,null);
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
    private static String _getProperty(String var,String default_value) {
        if(var == null)
            return null;
        List<String> list=parseCommaDelimitedStrings(var);
        if(list == null || list.isEmpty()) {
            list=new ArrayList<>(1);
            list.add(var);
        }
        String retval=null;
        for(String prop : list) {
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
        for(int i=0; i < bytes.length; i++) {
            byte b=bytes[i];
            sb.append(0x00FF & b);
            if(i + 1 < bytes.length) {
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
            MessageDigest md=MessageDigest.getInstance("MD5");
            byte[] bytes=md.digest(source.getBytes());
            return getString(bytes);
        }
        catch(Exception e) {
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
            MessageDigest md=MessageDigest.getInstance("SHA");
            byte[] bytes=md.digest(source.getBytes());
            return getString(bytes);
        }
        catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Converts a method name to an attribute name, e.g. getFooBar() --> foo_bar, isFlag --> flag.
     * @param methodName
     * @return
     */
    public static String methodNameToAttributeName(final String methodName) {
        String name=methodName;
        if((methodName.startsWith("get") || methodName.startsWith("set")) && methodName.length() > 3)
            name=methodName.substring(3);
        else if(methodName.startsWith("is") && methodName.length() > 2)
            name=methodName.substring(2);

        // Pattern p=Pattern.compile("[A-Z]+");
        Matcher m=METHOD_NAME_TO_ATTR_NAME_PATTERN.matcher(name);
        StringBuffer sb=new StringBuffer();
        while(m.find()) {
            int start=m.start(), end=m.end();
            String str=name.substring(start,end).toLowerCase();
            if(str.length() > 1) {
                String tmp1=str.substring(0,str.length() - 1);
                String tmp2=str.substring(str.length() - 1);
                str=tmp1 + "_" + tmp2;
            }
            if(start == 0) {
                m.appendReplacement(sb,str);
            }
            else
                m.appendReplacement(sb,"_" + str);
        }
        m.appendTail(sb);
        return sb.length() > 0? sb.toString() : methodName;
    }

    /**
     * Converts a method name to a Java attribute name, e.g. getFooBar() --> fooBar, isFlag --> flag.
     * @param methodName
     * @return
     */
    public static String methodNameToJavaAttributeName(final String methodName) {
        String name=methodName;
        if((methodName.startsWith("get") || methodName.startsWith("set")) && methodName.length() > 3)
            name=methodName.substring(3);
        else if(methodName.startsWith("is") && methodName.length() > 2)
            name=methodName.substring(2);

        if(Character.isUpperCase(name.charAt(0)))
            return name.substring(0,1).toLowerCase() + name.substring(1);
        return name;
    }


    public static String attributeNameToMethodName(String attr_name) {
        if(attr_name.contains("_")) {
            // Pattern p=Pattern.compile("_.");
            Matcher m=ATTR_NAME_TO_METHOD_NAME_PATTERN.matcher(attr_name);
            StringBuffer sb=new StringBuffer();
            while(m.find()) {
                m.appendReplacement(sb,attr_name.substring(m.end() - 1,m.end()).toUpperCase());
            }
            m.appendTail(sb);
            char first=sb.charAt(0);
            if(Character.isLowerCase(first)) {
                sb.setCharAt(0,Character.toUpperCase(first));
            }
            return sb.toString();
        }
        else {
            if(Character.isLowerCase(attr_name.charAt(0))) {
                return attr_name.substring(0,1).toUpperCase() + attr_name.substring(1);
            }
            else {
                return attr_name;
            }
        }
    }


}




