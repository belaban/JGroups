package org.jgroups.util;

import org.jgroups.*;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.Connection;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.*;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.management.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.System.nanoTime;
import static org.jgroups.protocols.TP.LIST;
import static org.jgroups.protocols.TP.MULTICAST;

/**
 * Collection of various utility routines that can not be assigned to other classes.
 * @author Bela Ban
 */
public class Util {
    private static final Map<Class<?>,Byte>   TYPES=new IdentityHashMap<>(30);
    private static final IntHashMap<Class<?>> CLASS_TYPES=new IntHashMap<>(20);

    private static final byte    TYPE_NULL         =  0;
    private static final byte    TYPE_BOOLEAN      =  1; // boolean
    private static final byte    TYPE_BOOLEAN_OBJ  =  2; // Boolean
    private static final byte    TYPE_BYTE         =  3; // byte
    private static final byte    TYPE_BYTE_OBJ     =  4; // Byte
    private static final byte    TYPE_CHAR         =  5; // char
    private static final byte    TYPE_CHAR_OBJ     =  6; // Character
    private static final byte    TYPE_DOUBLE       =  7; // double
    private static final byte    TYPE_DOUBLE_OBJ   =  8; // Double
    private static final byte    TYPE_FLOAT        =  9; // float
    private static final byte    TYPE_FLOAT_OBJ    = 10; // Float
    private static final byte    TYPE_INT          = 11; // int
    private static final byte    TYPE_INT_OBJ      = 12; // Integer
    private static final byte    TYPE_LONG         = 13; // long
    private static final byte    TYPE_LONG_OBJ     = 14; // Long
    private static final byte    TYPE_SHORT        = 15; // short
    private static final byte    TYPE_SHORT_OBJ    = 16; // Short
    private static final byte    TYPE_STRING       = 17; // ascii
    private static final byte    TYPE_BYTEARRAY    = 18;
    private static final byte    TYPE_CLASS        = 19; // a class

    private static final byte    TYPE_STREAMABLE   = 50;
    private static final byte    TYPE_SERIALIZABLE = 51;


    public static final int      MAX_PORT=65535; // highest port allocatable

    private static final Pattern METHOD_NAME_TO_ATTR_NAME_PATTERN=Pattern.compile("[A-Z]+");
    private static final Pattern ATTR_NAME_TO_METHOD_NAME_PATTERN=Pattern.compile("_.");


    protected static int         CCHM_INITIAL_CAPACITY=16;
    protected static float       CCHM_LOAD_FACTOR=0.75f;
    protected static int         CCHM_CONCURRENCY_LEVEL=16;
    public static final int      DEFAULT_HEADERS;

    public static final String   JAVA_VERSION;

    /**
     * The max size of an address list, used in View or Digest when toString() is called. Limiting this
     * reduces the amount of log data
     */
    public static int            MAX_LIST_PRINT_SIZE=20;

    private static final byte[]  TYPE_NULL_ARRAY={0};
    private static final byte[]  TYPE_BOOLEAN_TRUE={TYPE_BOOLEAN, 1};
    private static final byte[]  TYPE_BOOLEAN_FALSE={TYPE_BOOLEAN, 0};

    public static final Class<? extends Protocol>[] getUnicastProtocols() {return new Class[]{UNICAST3.class};}

    public enum AddressScope {GLOBAL,SITE_LOCAL,LINK_LOCAL,LOOPBACK,NON_LOOPBACK}

    private static boolean                          ipv4_stack_available=false, ipv6_stack_available=false;
    private static final StackType                  ip_stack_type;
    private static volatile List<NetworkInterface>  CACHED_INTERFACES=null;
    private static volatile Collection<InetAddress> CACHED_ADDRESSES=null;

    public static final boolean                     can_bind_to_mcast_addr;
    protected static ResourceBundle                 resource_bundle;
    static DateTimeFormatter                        UTF_FORMAT=DateTimeFormatter.ofPattern("E MMM d H:m:s 'UTC' y");
    protected static final String                   UUID_PREFIX="uuid://";
    protected static final String                   SITE_UUID_PREFIX="site-addr://";
    protected static final String                   IP_PREFIX="ip://";

    static {

        try {
            CACHED_INTERFACES=getAllAvailableInterfaces();
            CACHED_ADDRESSES=getAllAvailableAddresses();
        }
        catch(SocketException e) {
            throw new RuntimeException(e);
        }
        ip_stack_type=_getIpStackType();

        String tmp;
        resource_bundle=ResourceBundle.getBundle("jg-messages",Locale.getDefault(),Util.class.getClassLoader());

        add(TYPE_NULL, Void.class);
        add(TYPE_BOOLEAN,      boolean.class);
        add(TYPE_BOOLEAN_OBJ,  Boolean.class);
        add(TYPE_BYTE,         byte.class);
        add(TYPE_BYTE_OBJ,     Byte.class);
        add(TYPE_CHAR,         char.class);
        add(TYPE_CHAR_OBJ,     Character.class);
        add(TYPE_DOUBLE,       double.class);
        add(TYPE_DOUBLE_OBJ,   Double.class);
        add(TYPE_FLOAT,        float.class);
        add(TYPE_FLOAT_OBJ,    Float.class);
        add(TYPE_INT,          int.class);
        add(TYPE_INT_OBJ,      Integer.class);
        add(TYPE_LONG,         long.class);
        add(TYPE_LONG_OBJ,     Long.class);
        add(TYPE_SHORT,        short.class);
        add(TYPE_SHORT_OBJ,    Short.class);
        add(TYPE_STRING,       String.class);
        add(TYPE_BYTEARRAY,    byte[].class);
        add(TYPE_CLASS,        Class.class);

        can_bind_to_mcast_addr=(Util.checkForLinux() && !Util.checkForAndroid())
          || Util.checkForSolaris() || Util.checkForHp() || Util.checkForMac();

        try {
            String cchm_initial_capacity=SecurityActions.getProperty(Global.CCHM_INITIAL_CAPACITY);
            if(cchm_initial_capacity != null)
                CCHM_INITIAL_CAPACITY=Integer.parseInt(cchm_initial_capacity);
        }
        catch(SecurityException ignored) {
        }

        try {
            String cchm_load_factor=SecurityActions.getProperty(Global.CCHM_LOAD_FACTOR);
            if(cchm_load_factor != null)
                CCHM_LOAD_FACTOR=Float.parseFloat(cchm_load_factor);
        }
        catch(SecurityException ignored) {
        }

        try {
            String cchm_concurrency_level=SecurityActions.getProperty(Global.CCHM_CONCURRENCY_LEVEL);
            if(cchm_concurrency_level != null)
                CCHM_CONCURRENCY_LEVEL=Integer.parseInt(cchm_concurrency_level);
        }
        catch(SecurityException ignored) {
        }

        try {
            tmp=SecurityActions.getProperty(Global.MAX_LIST_PRINT_SIZE);
            if(tmp != null)
                MAX_LIST_PRINT_SIZE=Integer.parseInt(tmp);
        }
        catch(SecurityException ignored) {
        }

        try {
            tmp=SecurityActions.getProperty(Global.DEFAULT_HEADERS);
            DEFAULT_HEADERS=tmp != null? Integer.parseInt(tmp) : 4;
        }
        catch(Throwable t) {
            throw new IllegalArgumentException(String.format("property %s has an incorrect value", Global.DEFAULT_HEADERS), t);
        }

        JAVA_VERSION=System.getProperty("java.vm.version", "");
    }

    @Deprecated
    public static boolean fibersAvailable() {
        return ThreadCreator.hasVirtualThreads();
    }

    public static boolean virtualThreadsAvailable() {
        return ThreadCreator.hasVirtualThreads();
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
          new GMS().setJoinTimeout(1000),
          new FRAG2().setFragSize(8000)
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
    public static void waitUntilAllChannelsHaveSameView(long timeout, long interval, JChannel... channels) throws TimeoutException {
        if(interval >= timeout || timeout <= 0)
            throw new IllegalArgumentException("interval needs to be smaller than timeout or timeout needs to be > 0");
        if(channels == null || channels.length == 0)
            return;
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_channels_have_correct_view=true;
            View first=channels[0].getView();
            for(JChannel ch: channels) {
                View view=ch.getView();
                if(first == null || !Objects.equals(view, first) || view.size() != channels.length) {
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
            if(view == null || !Objects.equals(view, first) || view.size() != channels.length)
                throw new TimeoutException("Timeout " + timeout + " kicked in, views are:\n" + sb);
    }

    public static void waitUntilAllChannelsHaveSameView(long timeout, long interval,
                                                        Collection<JChannel> channels) throws TimeoutException {
        JChannel[] tmp=new JChannel[channels != null? channels.size() : 0];
        if(tmp == null)
            return;
        int index=0;
        for(Iterator<JChannel> it=channels.iterator(); it.hasNext();)
            tmp[index++]=it.next();
        waitUntilAllChannelsHaveSameView(timeout, interval, tmp);
    }

    public static void waitUntil(long timeout, long interval, BooleanSupplier condition) throws TimeoutException {
        waitUntil(timeout, interval, condition, null);
    }


    public static void waitUntil(long timeout, long interval, BooleanSupplier condition, Supplier<String> msg) throws TimeoutException {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            if(condition.getAsBoolean())
                return;
            Util.sleep(interval);
        }
        String error_msg=String.format("Timeout %d kicked in%s", timeout, msg != null? ": " + msg.get() : "");
        throw new TimeoutException(error_msg);
    }

    public static boolean waitUntilNoX(long timeout, long interval, BooleanSupplier condition) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            if(condition.getAsBoolean())
                return true;
            Util.sleep(interval);
        }
        return condition.getAsBoolean();
    }

    public static boolean waitUntilTrue(long timeout, long interval, BooleanSupplier condition) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            if(condition.getAsBoolean())
                return true;
            Util.sleep(interval);
        }
        return false;
    }

    /** In a separate thread (common ForkJoinPool): waits for the given timeout / interval until a condition is true.
     * When true, executes the on_success supplier, else the on_failure supplier */
    public static <T> void asyncWaitUntilTrue(long timeout, long interval, BooleanSupplier cond,
                                          Supplier<T> on_success, Supplier<T> on_failure) {
        CompletableFuture.supplyAsync(() -> Util.waitUntilTrue(timeout, interval, cond))
          .thenAccept(success -> {
              if(success)
                  on_success.get();
              else
                  on_failure.get();
          });
    }

    public static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
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
            sb.append(String.format("%s: %s\n", ch.name(), ch.getView()));
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

    public static void removeFromViews(Address mbr, JChannel ... channels) {
        if(mbr == null || channels == null)
            return;
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            if(gms == null) continue;
            View v=gms.view();
            if(v != null && v.containsMember(mbr)) {
                List<Address> mbrs=new ArrayList<>(v.getMembers());
                mbrs.remove(mbr);
                long id=v.getViewId().getId() + 1;
                View next=View.create(mbrs.get(0), id, mbrs);
                gms.installView(next);
            }
        }
    }

    /** Creates an enum from a string */
    public static <T extends Enum<T>> T createEnum(String name, Type type) {
        return Enum.valueOf((Class<T>) type, name);
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
            catch(Throwable ignored) {
            }
        }
    }


    public static void close(Closeable... closeables) {
        if(closeables != null) {
            for(Closeable closeable: closeables)
                Util.close(closeable);
        }
    }

    public static void closeReverse(Closeable... closeables) {
        if(closeables != null) {
            for(int i=closeables.length-1; i >= 0; i--)
                Util.close(closeables[i]);
        }
    }

    /** Closes all non-coordinators first, in parallel, then closes the coord. This should produce just 2 views */
    public static void closeFast(JChannel ... channels) {
        if(channels != null) {
            // close all non-coordinator channels first (in parallel)
            Stream.of(channels).parallel().filter(ch -> !isCoordinator(ch)).forEach(JChannel::close);
            try {
                Util.waitUntil(5000, 100,
                               () -> Stream.of(channels).filter(ch -> !isCoordinator(ch)).allMatch(JChannel::isClosed));
            }
            catch(TimeoutException e) {
            }
            // then close the cordinator's channel
            Stream.of(channels).filter(Util::isCoordinator).forEach(JChannel::close);
        }
    }

    /**
     * Drops messages to/from other members and then closes the channel. Note that this member won't get excluded from
     * the view until failure detection has kicked in and the new coord installed the new view
     */
    public static void shutdown(JChannel ch) throws Exception {
        DISCARD discard=new DISCARD();
        discard.setAddress(ch.getAddress());
        discard.discardAll(true);
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        stack.insertProtocol(discard,ProtocolStack.Position.ABOVE,transport.getClass());

        //abruptly shutdown FD_SOCK just as in real life when member gets killed non gracefully
        FD_SOCK fd=ch.getProtocolStack().findProtocol(FD_SOCK.class);
        if(fd != null)
            fd.stopServerSocket(false);

        View view=ch.getView();
        if(view != null) {
            ViewId vid=view.getViewId();
            List<Address> members=Collections.singletonList(ch.getAddress());

            ViewId new_vid=new ViewId(ch.getAddress(),vid.getId() + 1);
            View new_view=new View(new_vid,members);

            // inject view in which the shut-down member is the only element
            GMS gms=stack.findProtocol(GMS.class);
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
        return bits=(byte)(bits & ~flag);
    }

    public static String flagsToString(short flags) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;

        Message.Flag[] all_flags=Message.Flag.values();
        for(Message.Flag flag: all_flags) {
            if(isFlagSet(flags, flag)) {
                if(first)
                    first=false;
                else
                    sb.append("|");
                sb.append(flag);
            }
        }
        return sb.toString();
    }

    public static String transientFlagsToString(short flags) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;

        Message.TransientFlag[] all_flags=Message.TransientFlag.values();
        for(Message.TransientFlag flag: all_flags) {
            if(isTransientFlagSet(flags, flag)) {
                if(first)
                    first=false;
                else
                    sb.append("|");
                sb.append(flag);
            }
        }
        return sb.toString();
    }

    public static boolean isFlagSet(short flags, Message.Flag flag) {
        return flag != null && ((flags & flag.value()) == flag.value());
    }

    public static boolean isTransientFlagSet(short flags, Message.TransientFlag flag) {
        return flag != null && (flags & flag.value()) == flag.value();
    }

    /**
     * Copies a message. Copies only headers with IDs >= starting_id or IDs which are in the copy_only_ids list
     * @param copy_buffer
     * @param starting_id
     * @param copy_only_ids
     * @return
     */
    public static Message copy(Message msg, boolean copy_buffer, short starting_id, short... copy_only_ids) {
        Message retval=msg.copy(copy_buffer, false);
        for(Map.Entry<Short,Header> entry: msg.getHeaders().entrySet()) {
            short id=entry.getKey();
            if(id >= starting_id || Util.containsId(id, copy_only_ids))
                retval.putHeader(id, entry.getValue());
        }
        return retval;
    }


    /**
     * Creates an object from a byte buffer
     */
    public static <T extends Object> T objectFromByteBuffer(byte[] buffer) throws IOException, ClassNotFoundException {
        if(buffer == null) return null;
        return objectFromByteBuffer(buffer,0,buffer.length);
    }

    public static <T extends Object> T objectFromByteBuffer(byte[] buffer,int offset,int length) throws IOException, ClassNotFoundException {
        return objectFromByteBuffer(buffer, offset, length, null);
    }

    public static <T extends Object> T objectFromBuffer(ByteArray b, ClassLoader loader) throws IOException, ClassNotFoundException {
        return objectFromByteBuffer(b.getArray(), b.getOffset(), b.getLength(), loader);
    }

    public static <T extends Object> T objectFromByteBuffer(byte[] buffer,int offset,int length, ClassLoader loader) throws IOException, ClassNotFoundException {
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
                    return (T)oin.readObject();
                }
            case TYPE_BOOLEAN: case TYPE_BOOLEAN_OBJ: return (T)(Boolean)(buffer[offset] == 1);
            case TYPE_BYTE:    case TYPE_BYTE_OBJ:    return (T)(Byte)buffer[offset];
            case TYPE_CHAR:    case TYPE_CHAR_OBJ:    return (T)(Character)Bits.readChar(buffer, offset);
            case TYPE_DOUBLE:  case TYPE_DOUBLE_OBJ:  return (T)(Double)Bits.readDouble(buffer, offset);
            case TYPE_FLOAT:   case TYPE_FLOAT_OBJ:   return (T)(Float)Bits.readFloat(buffer, offset);
            case TYPE_INT:     case TYPE_INT_OBJ:     return (T)(Integer)Bits.readIntCompressed(buffer, offset);
            case TYPE_LONG:    case TYPE_LONG_OBJ:    return (T)(Long)Bits.readLongCompressed(buffer, offset);
            case TYPE_SHORT:   case TYPE_SHORT_OBJ:   return (T)(Short)Bits.readShort(buffer, offset);
            case TYPE_STRING:
                in=new ByteArrayDataInputStream(buffer, offset, length);
                return (T)readString(in);
            case TYPE_BYTEARRAY:
                byte[] tmp=new byte[length];
                System.arraycopy(buffer,offset,tmp,0,length);
                return (T)tmp;
            case TYPE_CLASS:
                return (T)CLASS_TYPES.get(buffer[offset]);
            default:
                throw new IllegalArgumentException("type " + type + " is invalid");
        }
    }

    /**
     * Parses an object from a {@link ByteBuffer}. Note that this changes the position of the buffer, so it this is
     * not desired, use {@link ByteBuffer#duplicate()} to create a copy and pass the copy to this method.
     */
    public static <T extends Object> T objectFromByteBuffer(ByteBuffer buffer, ClassLoader loader) throws Exception {
        if(buffer == null) return null;
        byte type=buffer.get();
        switch(type) {
            case TYPE_NULL:
                return null;
            case TYPE_STREAMABLE:
                DataInput in=new ByteBufferInputStream(buffer);
                return readGenericStreamable(in, loader);
            case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                InputStream in_stream=new ByteBufferInputStream(buffer);
                try(ObjectInputStream oin=new ObjectInputStreamWithClassloader(in_stream, loader)) {
                    return (T)oin.readObject();
                }
            case TYPE_BOOLEAN: case TYPE_BOOLEAN_OBJ: return (T)(Boolean)(buffer.get() == 1);
            case TYPE_BYTE:    case TYPE_BYTE_OBJ:    return (T)(Byte)buffer.get();
            case TYPE_CHAR:    case TYPE_CHAR_OBJ:    return (T)(Character)Bits.readChar(buffer);
            case TYPE_DOUBLE:  case TYPE_DOUBLE_OBJ:  return (T)(Double)Bits.readDouble(buffer);
            case TYPE_FLOAT:   case TYPE_FLOAT_OBJ:   return (T)(Float)Bits.readFloat(buffer);
            case TYPE_INT:     case TYPE_INT_OBJ:     return (T)(Integer)Bits.readIntCompressed(buffer);
            case TYPE_LONG:    case TYPE_LONG_OBJ:    return (T)(Long)Bits.readLongCompressed(buffer);
            case TYPE_SHORT:   case TYPE_SHORT_OBJ:   return (T)(Short)Bits.readShort(buffer);
            case TYPE_BYTEARRAY:
                byte[] tmp=new byte[buffer.remaining()];
                buffer.get(tmp);
                return (T)tmp;
            case TYPE_STRING:
                in=new ByteBufferInputStream(buffer);
                return (T)readString(in);
            case TYPE_CLASS:
                return (T)CLASS_TYPES.get(buffer.get());
            default:
                throw new IllegalArgumentException("type " + type + " is invalid");
        }
    }

    public static byte[] objectToByteBuffer(Object obj) throws IOException {
        return objectToBuffer(obj).getBytes();
    }

    /**
     * Serializes/Streams an object into a byte buffer.
     * The object has to implement interface Serializable or Externalizable or Streamable.
     */
    public static ByteArray objectToBuffer(Object obj) throws IOException {
        if(obj == null)
            return new ByteArray(TYPE_NULL_ARRAY);
        if(obj instanceof Streamable)
            return writeStreamable((Streamable)obj);
        Byte type=obj instanceof Class<?>? TYPES.get(obj) : TYPES.get(obj.getClass());
        return type == null? writeSerializable(obj) : new ByteArray(marshalPrimitiveType(type, obj));
    }

    protected static ByteArray writeStreamable(Streamable obj) throws IOException {
        int expected_size=obj instanceof SizeStreamable? ((SizeStreamable)obj).serializedSize() : 512;
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size, true);
        out.write(TYPE_STREAMABLE);
        writeGenericStreamable(obj, out);
        return out.getBuffer();
    }

    protected static ByteArray writeSerializable(Object obj) throws IOException {
        final ByteArrayDataOutputStream out_stream=new ByteArrayDataOutputStream(512, true);
        out_stream.write(TYPE_SERIALIZABLE);
        try(ObjectOutputStream out=new ObjectOutputStream(new OutputStreamAdapter(out_stream))) {
            out.writeObject(obj);
            out.flush();
            return out_stream.getBuffer();
        }
    }


    protected static byte[] marshalPrimitiveType(Byte type, Object obj) {
        if(obj instanceof Class<?>)
            return new byte[]{TYPE_CLASS, type};
        switch(type) {
            case TYPE_BOOLEAN: case TYPE_BOOLEAN_OBJ:
                return ((Boolean)obj)? TYPE_BOOLEAN_TRUE : TYPE_BOOLEAN_FALSE;
            case TYPE_BYTE: case TYPE_BYTE_OBJ:
                return new byte[]{type, (byte)obj};
            case TYPE_CHAR: case TYPE_CHAR_OBJ:
                byte[] buf=new byte[Global.BYTE_SIZE *3];
                buf[0]=type;
                Bits.writeChar((char)obj, buf, 1);
                return buf;
            case TYPE_DOUBLE: case TYPE_DOUBLE_OBJ:
                buf=new byte[Global.BYTE_SIZE + Global.DOUBLE_SIZE];
                buf[0]=type;
                Bits.writeDouble((double)obj, buf, 1);
                return buf;
            case TYPE_FLOAT: case TYPE_FLOAT_OBJ:
                buf=new byte[Global.BYTE_SIZE + Global.FLOAT_SIZE];
                buf[0]=type;
                Bits.writeFloat((float)obj, buf, 1);
                return buf;
            case TYPE_INT: case TYPE_INT_OBJ:
                int i=(int)obj;
                buf=new byte[Global.BYTE_SIZE + Bits.size(i)];
                buf[0]=type;
                Bits.writeIntCompressed(i, buf, 1);
                return buf;
            case TYPE_LONG: case TYPE_LONG_OBJ:
                long l=(long)obj;
                buf=new byte[Global.BYTE_SIZE + Bits.size(l)];
                buf[0]=type;
                Bits.writeLongCompressed((long)obj, buf, 1);
                return buf;
            case TYPE_SHORT: case TYPE_SHORT_OBJ:
                buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE];
                buf[0]=type;
                Bits.writeShort((short)obj, buf, 1);
                return buf;
            case TYPE_STRING:
                String str=(String)obj;
                ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(str.length()*2 +3);
                try {
                    out.writeByte(TYPE_STRING);
                    writeString(str, out);
                    byte[] ret=new byte[out.position()];
                    System.arraycopy(out.buffer(), 0, ret, 0, ret.length);
                    return ret;
                }
                catch(IOException ex) {
                    throw new RuntimeException(ex);
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

    public static void writeString(String str, DataOutput out) throws IOException {
        boolean is_ascii=isAsciiString(str);
        out.writeBoolean(is_ascii);
        if(is_ascii) {
            int len=str.length();
            out.writeInt(len);
            for(int i=0; i < len; i++)
                out.write((byte)str.charAt(i));
        }
        else
            out.writeUTF(str);
    }

    public static String readString(DataInput in) throws IOException {
        if(in.readBoolean() == false)
            return in.readUTF();
        byte[] tmp=new byte[in.readInt()];
        in.readFully(tmp);
        return new String(tmp);
    }

    public static int size(Object obj) {
        if(obj == null)
            return Global.BYTE_SIZE;
        if(obj instanceof SizeStreamable)
            return Global.BYTE_SIZE + Util.size((SizeStreamable)obj);
        return sizePrimitive(obj);
    }

    public static int size(SizeStreamable s) {
        int retval=Global.BYTE_SIZE;
        if(s == null)
            return retval;
        retval+=Global.SHORT_SIZE; // magic number
        short magic_number=ClassConfigurator.getMagicNumber(s.getClass());
        if(magic_number == -1)
            retval+=Bits.sizeUTF(s.getClass().getName());
        return retval + s.serializedSize();
    }

    public static int sizePrimitive(Object obj) {
        if(obj == null)
            return 1; // TYPE_NULL
        Byte type=obj instanceof Class<?>? TYPES.get(obj) : TYPES.get(obj.getClass());
        if(obj instanceof Class<?>) {
            if(type != null)
                return Global.BYTE_SIZE *2;
        }

        int retval=Global.BYTE_SIZE;
        switch(type) {
            case TYPE_BOOLEAN: case TYPE_BOOLEAN_OBJ:
            case TYPE_BYTE: case TYPE_BYTE_OBJ:
                return retval+Global.BYTE_SIZE;
            case TYPE_SHORT: case TYPE_SHORT_OBJ:
            case TYPE_CHAR: case TYPE_CHAR_OBJ:
                return retval+Character.BYTES;
            case TYPE_LONG: case TYPE_LONG_OBJ:
            case TYPE_DOUBLE: case TYPE_DOUBLE_OBJ:
                return retval+Double.BYTES;
            case TYPE_INT: case TYPE_INT_OBJ:
            case TYPE_FLOAT: case TYPE_FLOAT_OBJ:
                return retval+Float.BYTES;
            case TYPE_STRING:
                String s=(String)obj;
                retval+=Global.BYTE_SIZE; // is_ascii
                if(isAsciiString(s))
                    return retval+Global.INT_SIZE + s.length();
                else
                    return retval+Bits.sizeUTF(s);
            case TYPE_BYTEARRAY:
                byte[] buf=(byte[])obj;
                return retval+Global.INT_SIZE + buf.length;
            default:
                throw new IllegalArgumentException("type " + type + " is invalid");
        }
    }

    public static void writeTypeStreamable(Streamable obj, DataOutput out) throws IOException {
        out.writeByte(TYPE_STREAMABLE);
        Util.writeGenericStreamable(obj, out);
    }

    public static void primitiveToStream(Object obj, DataOutput out) throws IOException {
        if(obj == null) {
            out.write(TYPE_NULL);
            return;
        }
        Byte type=obj instanceof Class<?>? TYPES.get(obj) : TYPES.get(obj.getClass());
        if(obj instanceof Class<?>) {
            out.writeByte(TYPE_CLASS);
            out.writeByte(type);
            return;
        }

        out.writeByte(type);
        switch(type) {
            case TYPE_BOOLEAN: case TYPE_BOOLEAN_OBJ:
                out.writeBoolean((Boolean)obj);
                break;
            case TYPE_BYTE: case TYPE_BYTE_OBJ:
                out.writeByte((Byte)obj);
                break;
            case TYPE_CHAR: case TYPE_CHAR_OBJ:
                out.writeChar((Character)obj);
                break;
            case TYPE_DOUBLE: case TYPE_DOUBLE_OBJ:
                out.writeDouble((Double)obj);
                break;
            case TYPE_FLOAT: case TYPE_FLOAT_OBJ:
                out.writeFloat((Float)obj);
                break;
            case TYPE_INT: case TYPE_INT_OBJ:
                out.writeInt((Integer)obj);
                break;
            case TYPE_LONG: case TYPE_LONG_OBJ:
                out.writeLong((Long)obj);
                break;
            case TYPE_SHORT: case TYPE_SHORT_OBJ:
                out.writeShort((Short)obj);
                break;
            case TYPE_STRING:
                writeString((String)obj, out);
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

    public static <T> T primitiveFromStream(DataInput in) throws IOException {
        if(in == null) return null;
        byte b=in.readByte();
        return primitiveFromStream(in, b);
    }

    public static <T> T primitiveFromStream(DataInput in, byte type) throws IOException {
        if(in == null) return null;
        switch(type) {
            case TYPE_NULL:
                return null;
            case TYPE_BOOLEAN: case TYPE_BOOLEAN_OBJ: return (T)(Boolean)in.readBoolean();
            case TYPE_BYTE:    case TYPE_BYTE_OBJ:    return (T)(Byte)in.readByte();
            case TYPE_CHAR:    case TYPE_CHAR_OBJ:    return (T)(Character)in.readChar();
            case TYPE_DOUBLE:  case TYPE_DOUBLE_OBJ:  return (T)(Double)in.readDouble();
            case TYPE_FLOAT:   case TYPE_FLOAT_OBJ:   return (T)(Float)in.readFloat();
            case TYPE_INT:     case TYPE_INT_OBJ:     return (T)(Integer)in.readInt();
            case TYPE_LONG:    case TYPE_LONG_OBJ:    return (T)(Long)in.readLong();
            case TYPE_SHORT:   case TYPE_SHORT_OBJ:   return (T)(Short)in.readShort();
            case TYPE_STRING:                         return (T)readString(in);
            case TYPE_BYTEARRAY:
                byte[] tmpbuf=new byte[in.readInt()];
                in.readFully(tmpbuf);
                return (T)tmpbuf;
            case TYPE_CLASS:
                return (T)CLASS_TYPES.get(in.readByte());
            default:
                throw new IllegalArgumentException("type " + type + " is invalid");
        }
    }

    public static void objectToStream(Object obj, DataOutput out) throws IOException {
        if(obj == null) {
            out.write(TYPE_NULL);
            return;
        }
        if(obj instanceof Streamable) {
            writeTypeStreamable((Streamable) obj, out);
            return;
        }
        Byte type=obj instanceof Class<?>? TYPES.get(obj) : TYPES.get(obj.getClass());
        if(type == null) {
            out.write(TYPE_SERIALIZABLE);
            try(ObjectOutputStream tmp=new ObjectOutputStream(out instanceof ByteArrayDataOutputStream?
                                                                new OutputStreamAdapter((ByteArrayDataOutputStream)out) :
                                                                (OutputStream)out)) {
                tmp.writeObject(obj);
                return;
            }
        }
        primitiveToStream(obj, out);
    }

    public static <T extends Object> T objectFromStream(DataInput in) throws IOException, ClassNotFoundException {
        return objectFromStream(in, null);
    }

    public static <T extends Object> T objectFromStream(DataInput in, ClassLoader loader) throws IOException, ClassNotFoundException {
        if(in == null) return null;
        byte b=in.readByte();

        switch(b) {
            case TYPE_STREAMABLE:
                return readGenericStreamable(in, loader);
            case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
                InputStream is=in instanceof ByteArrayDataInputStream?
                  new org.jgroups.util.InputStreamAdapter((ByteArrayDataInputStream)in) : (InputStream)in;
                try(ObjectInputStream tmp=new ObjectInputStreamWithClassloader(is, loader)) {
                    return (T)tmp.readObject();
                }
        }
        return primitiveFromStream(in, b);
    }


    public static <T extends Streamable> T streamableFromByteBuffer(Class<? extends Streamable> cl,byte[] buffer)
      throws Exception {
        return streamableFromByteBuffer(cl,buffer,0,buffer.length);
    }

    public static <T extends Streamable> T streamableFromByteBuffer(Supplier<T> factory, byte[] buffer) throws Exception {
        return streamableFromByteBuffer(factory, buffer, 0, buffer.length);
    }

    /**
     * Poor man's serialization of an exception. Serializes only the message, stack trace and cause (not suppressed exceptions)
     */
    public static void exceptionToStream(Throwable t, DataOutput out) throws IOException {
        Set<Throwable> causes=new HashSet<>();
        exceptionToStream(causes, t, out);
    }

    protected static void exceptionToStream(Set<Throwable> causes, Throwable t, DataOutput out) throws IOException {
        // 1. null check
        if( t == null) {
            out.writeBoolean(true);
            return;
        }
        out.writeBoolean(false);

        // 2. InvocationTargetException
        boolean invocation_target_ex=t instanceof InvocationTargetException;
        out.writeBoolean(invocation_target_ex);
        if(invocation_target_ex) {
            writeException(causes, t.getCause(), out);
            return;
        }

        writeException(causes, t, out);
    }

    public static ByteArray exceptionToBuffer(Throwable t) throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512, true);
        exceptionToStream(t, out);
        return out.getBuffer();
    }


    public static Throwable exceptionFromStream(DataInput in) throws IOException, ClassNotFoundException {
        return exceptionFromStream(in, 0);
    }


    protected static Throwable exceptionFromStream(DataInput in, int recursion_count) throws IOException, ClassNotFoundException {
        // 1. null check
        if(in.readBoolean())
            return null;

        // 2. InvocationTargetException
        if(in.readBoolean())
            return new InvocationTargetException(readException(in, recursion_count));
        return readException(in, recursion_count);
    }


    protected static void add(byte type, Class<?> cl) {
        if(TYPES.putIfAbsent(cl, type) != null)
            throw new IllegalStateException(String.format("type %d (class=%s) is already present in types map", type, cl));
        CLASS_TYPES.put(type, cl);
    }



    protected static void writeException(Set<Throwable> causes, Throwable t, DataOutput out) throws IOException {
        // 3. classname
        Bits.writeString(t.getClass().getName(), out);

        // 4. message
        Bits.writeString(t.getMessage(), out);

        // 5. stack trace
        StackTraceElement[] stack_trace=t.getStackTrace();
        int depth=stack_trace == null? 0 : stack_trace.length;
        out.writeShort(depth);
        if(depth > 0) {
            for(int i=0; i < stack_trace.length; i++) {
                StackTraceElement trace=stack_trace[i];
                Bits.writeString(trace.getClassName(), out);
                Bits.writeString(trace.getMethodName(), out);
                Bits.writeString(trace.getFileName(), out);
                Bits.writeIntCompressed(trace.getLineNumber(), out);
            }
        }

        // 6. cause
        Throwable cause=t.getCause();
        boolean serialize_cause=cause != null && causes.add(cause);
        out.writeBoolean(serialize_cause);
        if(serialize_cause)
            exceptionToStream(causes, cause, out);
    }


    protected static Throwable readException(DataInput in, int recursion_count) throws IOException, ClassNotFoundException {
        // 3. classname
        String classname=Bits.readString(in);
        Class<? extends Throwable> clazz=(Class<? extends Throwable>)Util.loadClass(classname, (ClassLoader)null);

        // 4. message
        String message=Bits.readString(in);

        Throwable retval=null;
        Constructor<? extends Throwable> ctor=null;

        try {
            ctor=clazz.getDeclaredConstructor(String.class);
            if(ctor != null) {
                if(!ctor.isAccessible())
                    ctor.setAccessible(true);
                retval=ctor.newInstance(message);
            }
        }
        catch(Throwable ignored) {
        }

        if(retval == null) {
            try {
                retval=clazz.getDeclaredConstructor().newInstance();
            } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

    // 5. stack trace
        int depth=in.readShort();
        if(depth > 0) {
            StackTraceElement[] stack_trace=new StackTraceElement[depth];
            for(int i=0; i < depth; i++) {
                String class_name=Bits.readString(in);
                String method_name=Bits.readString(in);
                String filename=Bits.readString(in);
                int line_number=Bits.readIntCompressed(in);
                StackTraceElement trace=new StackTraceElement(class_name, method_name, filename, line_number);
                stack_trace[i]=trace;
            }
            retval.setStackTrace(stack_trace);
        }

        // 6. cause
        if(in.readBoolean()) {
            // if malicious code constructs an exception whose causes have circles, then that would blow up the stack, so
            // we make sure here that we stop after a certain number of recursive calls
            if(++recursion_count > 20)
                throw new IllegalStateException(String.format("failed deserializing exception: recursion count=%d",
                                                              recursion_count));
            Throwable cause=exceptionFromStream(in, recursion_count);
            try {
                retval.initCause(cause); // will throw an exception if cause is already set
            }
            catch(Throwable t) {
                return retval;
            }
        }
        return retval;
    }


    public static Throwable exceptionFromBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, offset,length);
        return exceptionFromStream(in);
    }

    /** Returns a copy of the byte array between position and limit; requires a non-null buffer */
    public static byte[] bufferToArray(final ByteBuffer buf) {
        if(buf == null)
            return null;
        ByteBuffer tmp=buf.duplicate();
        int length=tmp.remaining();
        byte[] retval=new byte[length];
        tmp.get(retval, 0, retval.length);
        return retval;
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

    public static <T extends Streamable> T streamableFromByteBuffer(Class<? extends Streamable> cl,byte[] buffer,int offset,int length) throws Exception {
        if(buffer == null) return null;
        DataInput in=new ByteArrayDataInputStream(buffer,offset,length);
        T retval=(T)cl.getDeclaredConstructor().newInstance();
        retval.readFrom(in);
        return retval;
    }

    public static <T extends Streamable> T streamableFromByteBuffer(Class<? extends Streamable> cl, ByteBuffer buffer) throws Exception {
        if(buffer == null) return null;
        DataInput in=new ByteBufferInputStream(buffer);
        T retval=(T)cl.newInstance();
        retval.readFrom(in);
        return retval;
    }

    public static <T extends Streamable> T streamableFromByteBuffer(Supplier<T> factory, byte[] buffer, int offset, int length) throws Exception {
        if(buffer == null) return null;
        DataInput in=new ByteArrayDataInputStream(buffer,offset,length);
        T retval=factory.get();
        retval.readFrom(in);
        return retval;
    }

    public static <T extends Streamable> T streamableFromBuffer(Supplier<T> factory, byte[] buf, int off, int len)
      throws IOException, ClassNotFoundException {
        DataInput in=new ByteArrayDataInputStream(buf,off,len);
        return Util.readStreamable(factory, in);
    }

    public static byte[] streamableToByteBuffer(Streamable obj) throws IOException {
        int expected_size=obj instanceof SizeStreamable? ((SizeStreamable)obj).serializedSize() : 512;
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size);
        obj.writeTo(out);
        return Arrays.copyOf(out.buffer(), out.position());
    }

    public static ByteArray streamableToBuffer(Streamable obj) throws Exception {
        int expected_size=obj instanceof SizeStreamable? ((SizeStreamable)obj).serializedSize() +1 : 512;
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size);
        Util.writeStreamable(obj,out);
        return out.getBuffer();
    }

    public static ByteArray messageToBuffer(Message msg) throws Exception {
        int expected_size=msg.size() + Global.SHORT_SIZE; // type
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size);
        out.writeShort(msg.getType());
        msg.writeTo(out);
        return out.getBuffer();
    }


    public static Message messageFromBuffer(byte[] buf, int offset, int length, MessageFactory mf) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, offset, length);
        short type=in.readShort();
        Message msg=mf.create(type);
        msg.readFrom(in);
        return msg;
    }

    public static ByteArray messageToByteBuffer(Message msg) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(msg.size() +2);
        out.writeBoolean(msg != null);
        if(msg != null) {
            out.writeShort(msg.getType());
            msg.writeTo(out);
        }
        return out.getBuffer();
    }


    public static Message messageFromByteBuffer(byte[] buffer, int offset, int length, MessageFactory mf) throws Exception {
        DataInput in=new ByteArrayDataInputStream(buffer,offset,length);
        if(!in.readBoolean())
            return null;
        short type=in.readShort();
        Message msg=mf.create(type);
        msg.readFrom(in);
        return msg;
    }

    /** Tries to return a legible representation of a message's payload */
    public static String getObject(Message msg) {
        if(msg == null)
            return "null";
        if(!msg.hasPayload())
            return msg.printHeaders();
        try {
            Object obj=msg.getObject();
            return obj != null? obj.toString() : "null";
        }
        catch(Throwable t) {
            return String.format("<%d bytes>", msg.getLength());
        }
    }

    public static String getObjects(Iterable<Message> it) {
        if(it == null)
            return "0";
        return StreamSupport.stream(it.spliterator(), false)
          .map(Message::getObject)
          .map(Object::toString).collect(Collectors.joining(", "));
    }

    public static ByteBuffer wrapDirect(byte[] array) {
        return wrapDirect(array, 0, array.length);
    }

    public static ByteBuffer wrapDirect(byte[] array, int offset, int length) {
        ByteBuffer b=ByteBuffer.allocateDirect(length).put(array, offset, length);
        b.flip();
        return b;
    }

    public static byte[] collectionToByteBuffer(Collection<Address> c) throws IOException {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream((int)Util.size(c));
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

    public static String hexToBin(String s) {
        return new BigInteger(s, 16).toString(2);
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

    public static Object convert(String arg, Class<?> type, TimeUnit unit) {
        if(type == String.class) return arg;
        if(type == boolean.class || type == Boolean.class) return Boolean.valueOf(arg);
        if(type == byte.class    || type == Byte.class)    return Byte.valueOf(arg);
        if(type == short.class   || type == Short.class)   return Short.valueOf(arg);
        if(type == int.class     || type == Integer.class) return Integer.valueOf(arg);
        if(type == long.class    || type == Long.class)
            return unit != null? Util.readDurationLong(arg, unit) : Long.parseLong(arg);
        if(type == float.class   || type == Float.class)   return Float.valueOf(arg);
        if(type == double.class  || type == Double.class)  return Double.valueOf(arg);
        if(arg == null || arg.equals("null"))
            return null;
        if(type.isEnum()) {
            return Util.createEnum(arg, type);
        }
        return arg;
    }



    public static void writeMessage(Message msg, DataOutput dos, boolean multicast) throws IOException {
        byte flags=0;
        dos.writeShort(Version.version); // write the version
        if(multicast)
            flags+=MULTICAST;
        dos.writeByte(flags);
        dos.writeShort(msg.getType());
        msg.writeTo(dos);
    }

    public static Message readMessage(DataInput in, MessageFactory mf) throws IOException, ClassNotFoundException {
        short type=in.readShort();
        Message msg=mf.create(type);
        msg.readFrom(in);
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
     */
    public static void writeMessageList(Address dest, Address src, byte[] cluster_name, List<Message> msgs,
                                        DataOutput dos, boolean multicast) throws IOException {
        writeMessageListHeader(dest, src, cluster_name, msgs != null ? msgs.size() : 0, dos, multicast);

        if(msgs != null)
            for(Message msg: msgs) {
                dos.writeShort(msg.getType());
                msg.writeToNoAddrs(src, dos);
            }
    }


    public static void writeMessageList(Address dest, Address src, byte[] cluster_name, FastArray<Message> msgs,
                                        DataOutput dos, boolean multicast) throws IOException {
        writeMessageListHeader(dest, src, cluster_name, msgs != null ? msgs.size() : 0, dos, multicast);

        if(msgs != null)
            for(Message msg: msgs) {
                dos.writeShort(msg.getType());
                msg.writeToNoAddrs(src, dos);
            }
    }


    public static void writeMessageList(Address dest, Address src, byte[] cluster_name,
                                        Message[] msgs, int offset, int length, DataOutput dos, boolean multicast)
      throws IOException {
        writeMessageListHeader(dest, src, cluster_name, length, dos, multicast);

        if(msgs != null)
            for(int i=0; i < length; i++) {
                Message msg=msgs[offset+i];
                dos.writeShort(msg.getType());
                msg.writeToNoAddrs(src, dos);
            }
    }

    public static void writeMessageListHeader(Address dest, Address src, byte[] cluster_name, int numMsgs, DataOutput dos, boolean multicast) throws IOException {
        dos.writeShort(Version.version);

        byte flags=LIST;
        if(multicast)
            flags+=MULTICAST;

        dos.writeByte(flags);

        Util.writeAddress(dest, dos);

        Util.writeAddress(src, dos);

        dos.writeShort(cluster_name != null? cluster_name.length : -1);
        if(cluster_name != null)
            dos.write(cluster_name);

        dos.writeInt(numMsgs);
    }


    public static List<Message> readMessageList(DataInput in, short transport_id, MessageFactory mf)
      throws IOException, ClassNotFoundException {
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
            short type=in.readShort(); // skip the
            Message msg=mf.create(type);
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
     * Reads a list of messages into 2 MessageBatches:
     * <ol>
     *     <li>regular</li>
     *     <li>OOB</li>
     * </ol>
     * @return an array of 2 MessageBatches in the order above, the first batch is at index 0
     */
    public static MessageBatch[] readMessageBatch(DataInput in, boolean multicast, MessageFactory factory)
      throws IOException, ClassNotFoundException {
        MessageBatch[] batches=new MessageBatch[2]; // [0]: reg, [1]: OOB
        Address dest=Util.readAddress(in);
        Address src=Util.readAddress(in);
        short length=in.readShort();
        byte[] cluster_name=length >= 0? new byte[length] : null;
        if(cluster_name != null)
            in.readFully(cluster_name, 0, cluster_name.length);

        int len=in.readInt();
        for(int i=0; i < len; i++) {
            short type=in.readShort();
            Message msg=factory.create(type).setDest(dest).setSrc(src);
            msg.readFrom(in);
            boolean oob=msg.isFlagSet(Message.Flag.OOB);
            int index=0;
            MessageBatch.Mode mode=MessageBatch.Mode.REG;
            if(oob) {
                mode=MessageBatch.Mode.OOB;
                index=1;
            }
            if(batches[index] == null)
                batches[index]=new MessageBatch(dest, src, cluster_name != null? new AsciiString(cluster_name) : null, multicast, mode, len);
            batches[index].add(msg);
        }
        return batches;
    }

    public static void parse(byte[] buf, int offset, int length, BiConsumer<Short,Message> msg_consumer,
                        BiConsumer<Short,MessageBatch> batch_consumer, Consumer<GossipData> gossip_consumer,
                             boolean tcp, boolean gossip) {
        parse(new ByteArrayInputStream(buf, offset, length), msg_consumer, batch_consumer, gossip_consumer, tcp, gossip);
    }

    public static void parse(InputStream input, BiConsumer<Short,Message> msg_consumer,
                             BiConsumer<Short,MessageBatch> batch_consumer, Consumer<GossipData> gossip_consumer,
                             boolean tcp, boolean gossip) {
        if(msg_consumer == null && batch_consumer == null)
            return;
        byte[] tmp=new byte[Global.INT_SIZE];
        MessageFactory mf=new DefaultMessageFactory();
        try(DataInputStream dis=new DataInputStream(input)) {
            for(;;) {
                // for TCP, we send the length first; this needs to be skipped as it is not part of the JGroups payload
                if(tcp) { // TCP / TCP_NIO2
                    dis.readFully(tmp);
                    if(Arrays.equals(Connection.cookie, tmp)) {
                        // connection establishment; parse version (short) and IpAddress
                        dis.readShort();  // version
                        dis.readShort(); // address length (only needed by TCP_NIO2)
                        IpAddress peer=new IpAddress();
                        peer.readFrom(dis);
                        continue;
                    }
                    else {
                        // do nothing - the 4 bytes were the length
                        // int len=Bits.readInt(tmp, 0);
                    }
                }
                if(gossip) { // messages to or from a GossipRouter
                    GossipData g=new GossipData();
                    g.readFrom(dis, true, false);
                    if(g.getType() != GossipType.MESSAGE) {
                        if(gossip_consumer != null)
                            gossip_consumer.accept(g);
                        continue;
                    }
                }
                short version=dis.readShort();
                byte flags=dis.readByte();
                boolean is_message_list=(flags & LIST) == LIST;
                boolean multicast=(flags & MULTICAST) == MULTICAST;
                if(is_message_list) { // used if message bundling is enabled
                    final MessageBatch[] batches=Util.readMessageBatch(dis,multicast, mf);
                    for(MessageBatch batch: batches) {
                        if(batch == null)
                            continue;
                        if(batch_consumer != null)
                            batch_consumer.accept(version, batch);
                        else {
                            for(Message msg: batch)
                                msg_consumer.accept(version, msg);
                        }
                    }
                }
                else {
                    Message msg=Util.readMessage(dis, mf);
                    if(msg_consumer != null)
                        msg_consumer.accept(version, msg);
                }
            }
        }
        catch(EOFException ignored) {
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }



    public static void writeView(View view,DataOutput out) throws IOException {
        if(view == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeBoolean(view instanceof MergeView);
        view.writeTo(out);
    }


    public static View readView(DataInput in) throws IOException, ClassNotFoundException {
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

    public static void writeViewId(ViewId vid,DataOutput out) throws IOException {
        if(vid == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        vid.writeTo(out);
    }

    public static ViewId readViewId(DataInput in) throws IOException, ClassNotFoundException {
        if(!in.readBoolean())
            return null;
        ViewId retval=new ViewId();
        retval.readFrom(in);
        return retval;
    }


    public static void writeAddress(Address addr,DataOutput out) throws IOException {
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
        else if(addr.getClass().equals(IpAddress.class))
            flags=Util.setFlag(flags,Address.IP_ADDR);
        else
            streamable_addr=false;
        out.writeByte(flags);
        if(streamable_addr)
            addr.writeTo(out);
        else
            writeOtherAddress(addr,out);
    }

    public static Address readAddress(DataInput in) throws IOException, ClassNotFoundException {
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
        else
            addr=readOtherAddress(in);
        return addr;
    }

    public static int size(Address addr) {
        int retval=Global.BYTE_SIZE; // flags
        if(addr == null)
            return retval;

        if(addr instanceof UUID) {
            Class<? extends Address> clazz=addr.getClass();
            if(clazz.equals(UUID.class) || clazz.equals(SiteUUID.class) || clazz.equals(SiteMaster.class))
                return retval + addr.serializedSize();
        }
        if(addr instanceof IpAddress)
            return retval + addr.serializedSize();

        retval+=Global.SHORT_SIZE; // magic number
        retval+=addr.serializedSize();
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
        return buf == null? Global.BYTE_SIZE : Global.BYTE_SIZE + Global.INT_SIZE + buf.length;
    }

    private static Address readOtherAddress(DataInput in) throws IOException, ClassNotFoundException {
        short magic_number=in.readShort();
        Address addr=ClassConfigurator.create(magic_number);
        addr.readFrom(in);
        return addr;
    }

    private static void writeOtherAddress(Address addr,DataOutput out) throws IOException {
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
    public static void writeAddresses(Collection<? extends Address> v,DataOutput out) throws IOException {
        if(v == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(v.size());
        for(Address addr : v) {
            Util.writeAddress(addr,out);
        }
    }

    public static void writeAddresses(final Address[] addrs,DataOutput out) throws IOException {
        if(addrs == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(addrs.length);
        for(Address addr : addrs)
            Util.writeAddress(addr,out);
    }


    public static <T extends Collection<Address>> T readAddresses(DataInput in, IntFunction<T> factory)
      throws IOException, ClassNotFoundException {
        short length=in.readShort();
        if(length < 0) return null;
        T retval = factory.apply(length);
        Address addr;
        for(int i=0; i < length; i++) {
            addr=Util.readAddress(in);
            retval.add(addr);
        }
        return retval;
    }


    public static Address[] readAddresses(DataInput in) throws IOException, ClassNotFoundException {
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

    public static String addressToString(Address addr) {
        if(addr == null)
            return null;
        if(addr.isSiteAddress()) { // SiteUUID
            SiteUUID su=(SiteUUID)addr;
            return String.format("%s%s:%s:%s", SITE_UUID_PREFIX, su.toStringLong(), su.getName(), su.getSite());
        }
        Class<? extends Address> cl=addr.getClass();
        if(UUID.class.equals(cl))
            return String.format("%s%s", UUID_PREFIX, ((UUID)addr).toStringLong());
        if(IpAddress.class.equals(cl))
            return String.format("%s%s", IP_PREFIX, addr);
        return null;
    }

    public static Address addressFromString(String s) throws Exception {
        if(s == null)
            return null;
        int index=s.indexOf(UUID_PREFIX);
        if(index >= 0)
            return UUID.fromString(s.substring(index+UUID_PREFIX.length()));
        index=s.indexOf(SITE_UUID_PREFIX);
        if(index >= 0) {
            String[] tmp=s.substring(index + SITE_UUID_PREFIX.length()).split(":");
            if(tmp.length == 1)
                return UUID.fromString(tmp[0]);
            UUID u=UUID.fromString(tmp[0]);
            return new SiteUUID(u, tmp[1], tmp[2]);
        }
        index=s.indexOf(IP_PREFIX);
        if(index >= 0)
            return new IpAddress(s.substring(index + IP_PREFIX.length()));
        return null;


    }

    public static void writeStreamable(Streamable obj,DataOutput out) throws IOException {
        if(obj == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        obj.writeTo(out);
    }


    public static <T extends Streamable> T readStreamable(Supplier<T> factory, DataInput in) throws IOException, ClassNotFoundException {
        T retval=null;
        if(!in.readBoolean())
            return null;
        retval=factory.get();
        retval.readFrom(in);
        return retval;
    }


    public static void writeGenericStreamable(Streamable obj, DataOutput out) throws IOException {
        if(obj == null) {
            out.write(0);
            return;
        }
        out.write(1);
        short magic_number=ClassConfigurator.getMagicNumber(obj.getClass());
        out.writeShort(magic_number);
        if(magic_number == -1) {
            String classname=obj.getClass().getName();
            out.writeUTF(classname);
        }
        obj.writeTo(out); // write the contents
    }

    public static <T extends Streamable> T readGenericStreamable(DataInput in) throws IOException, ClassNotFoundException {
        return readGenericStreamable(in, null);
    }

    public static <T extends Streamable> T readGenericStreamable(DataInput in, ClassLoader loader) throws IOException, ClassNotFoundException {
        T retval=null;
        int b=in.readByte();
        if(b == 0)
            return null;

        short magic_number=in.readShort();

        Class<?> clazz;
        if(magic_number != -1) {
            retval=ClassConfigurator.create(magic_number);
        }
        else {
            String classname=in.readUTF();
            clazz=ClassConfigurator.get(classname, loader);
            try {
                retval=(T)clazz.getDeclaredConstructor().newInstance();
            }
            catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }
        retval.readFrom(in);
        return retval;
    }


    public static SizeStreamable readSizeStreamable(DataInput in, ClassLoader loader) throws IOException, ClassNotFoundException {
        int b=in.readByte();
        if(b == 0)
            return null;

        SizeStreamable retval=null;
        short magic_number=in.readShort();
        if(magic_number != -1) {
            retval=ClassConfigurator.create(magic_number);
        }
        else {
            String classname=in.readUTF();
            Class<SizeStreamable> clazz=(Class<SizeStreamable>)ClassConfigurator.get(classname, loader);
            try {
                retval=clazz.getDeclaredConstructor().newInstance();
            }
            catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        retval.readFrom(in);
        return retval;
    }



    public static <T extends Streamable> void write(T[] array, DataOutput out) throws IOException {
        Bits.writeIntCompressed(array != null? array.length : 0, out);
        if(array == null)
            return;
        for(T el: array)
            el.writeTo(out);
    }

    public static <T extends Streamable> T[] read(Class<T> clazz, DataInput in) throws Exception {
        int size=Bits.readIntCompressed(in);
        if(size == 0)
            return null;
        T[] retval=(T[])Array.newInstance(clazz, size);

        for(int i=0; i < retval.length; i++) {
            retval[i]=clazz.getDeclaredConstructor().newInstance();
            retval[i].readFrom(in);
        }
        return retval;
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
        byte[] contents=new byte[10000];
        byte[] buf=new byte[1024];
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
        return sb.length() > 0? sb.toString() : null;
    }

    /** Returns all characters read from the current position until the next occurrence of 'c' has been read
     * (including 'c'), or eof, whichever comes first */
    public static String readTillMatchingCharacter(InputStream in, char c) throws IOException {
        StringBuilder sb=new StringBuilder();
        boolean eof=false;
        for(;;) {
            int ch=in.read();
            if(ch == -1) {
                eof=true;
                break;
            }
            sb.append((char)ch);
            if(ch == c)
                break;
        }

        String retval=sb.toString().trim();
        return eof && retval.isEmpty()? null : retval;
    }


    public static String readStringFromStdin(String message) throws Exception {
        System.out.print(message);
        System.out.flush();
        System.in.skip(available(System.in));
        BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
        String line=reader.readLine();
        return line != null? line.trim() : null;
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


    public static void writeByteBuffer(byte[] buf,DataOutput out) throws IOException {
        writeByteBuffer(buf,0,buf.length,out);
    }

    public static void writeByteBuffer(byte[] buf, int offset, int length, DataOutput out) throws IOException {
        if(buf != null) {
            out.write(1);
            out.writeInt(length);
            out.write(buf,offset,length);
        }
        else
            out.write(0);
    }

    public static byte[] readByteBuffer(DataInput in) throws IOException {
        int b=in.readByte();
        if(b == 1) {
            b=in.readInt();
            byte[] buf=new byte[b];
            in.readFully(buf, 0, buf.length);
            return buf;
        }
        return null;
    }


    public static <T> boolean match(T obj1,T obj2) {
        if(obj1 == null && obj2 == null)
            return true;
        if(obj1 != null)
            return obj1.equals(obj2);
        else
            return obj2.equals(obj1);
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
            long initialTime=nanoTime() - 200;
            while(nanoTime() < initialTime + nanos) ;
        }
    }


    public static int keyPress(String msg) {
        System.out.println(msg);

        try {
            int ret=System.in.read();
            System.in.skip(available(System.in));
            return ret;
        }
        catch(IOException e) {
            return -1;
        }
    }

    public static int available(InputStream in) {
        try {
            return in.available();
        }
        catch(Exception ex) {
            return 0;
        }
    }

    public static long micros() {
        return nanoTime() / 1000;
    }

    public static int factorial(int n) {
        if(n == 1) return n;
        return n * factorial(n-1);
    }

    /**
     * Inefficient permutation of a generic list; uses too much copying among other things. PR welcome! :-)
     * @param list The list
     * @param permutations All permutations will be added to this list (as copies)
     * @param <E> the type of the list elements
     */
    public static <E> void permute(List<E> list, List<List<E>> permutations) {
        if(list.size() == 1) {
            permutations.add(list);
            return;
        }
        for(int i=0; i < list.size(); i++) {
            permute(null, list, i, permutations);
        }
    }

    protected static <E> void permute(List<E> prefix, List<E> list, int index, final List<List<E>> permutations) {
        if(list.size() <= 1)
            permutations.add(combine(prefix, list));
        else {
            List<E> swapped=swap(list, index);
            List<E> first=car(swapped);
            List<E> rest=cdr(swapped);
            for(int i=0; i < rest.size(); i++)
                permute(combine(prefix, first), rest, i, permutations);
        }
    }

    public static <E> List<E> car(List<E> l) {
        return l.isEmpty()? Collections.emptyList() : Collections.singletonList(l.get(0));
    }

    public static <E> List<E> cdr(List<E> l) {
        return l.isEmpty()? Collections.emptyList() : l.subList(1, l.size());
    }

    public static <E> List<E> combine(List<E> l1, List<E> l2) {
        ArrayList<E> retval=new ArrayList<>();
        if(l1 != null)
            retval.addAll(l1);
        if(l2 != null)
            retval.addAll(l2);
        return retval;
    }

    @SafeVarargs
    public static <E> E[] combine(E[] ... arrays) {
        if(arrays == null)
            return null;
        int size=(int)Stream.of(arrays).flatMap(Stream::of).map(Objects::nonNull).count();
        E[] retval=(E[])Array.newInstance(arrays[0].getClass().getComponentType(), size);
        AtomicInteger index=new AtomicInteger();
        Stream.of(arrays).flatMap(Stream::of).forEach(el -> retval[index.getAndIncrement()]=el);
        return retval;
    }

    @SafeVarargs
    public static <E> E[] combine(Class<?> cl, E ... array) {
        if(array == null)
            return null;
        int size=(int)Stream.of(array).filter(Objects::nonNull).count();
        E[] retval=(E[])Array.newInstance(cl, size);
        AtomicInteger index=new AtomicInteger();
        Stream.of(array).filter(Objects::nonNull).forEach(el -> retval[index.getAndIncrement()]=el);
        return retval;
    }

    // moves el at index to the front, returns a copy
    protected static <E> List<E> swap(List<E> l, int index) {
        List<E> swapped=new ArrayList<>();
        E el=l.get(index);
        swapped.add(el);
        for(int i=0; i < l.size(); i++) {
            el=l.get(i);
            if(i != index)
                swapped.add(el);
        }
        return swapped;
    }


    /**
     * Performs an ordered permutation of the elements of a and b such that the order of elements in list a and b is
     * preserved. Example: {A1,A2} and {B1,B2} -> {A1,B1,A2,B2} but not {A1,B2,B1,A2}
     */
    public static <E> Collection<List<E>> orderedPermutation(List<E> a, List<E> b) {
        Collection<List<E>> perms=new LinkedHashSet<>();
        for(int i=0; i <= a.size(); i++) {
            for(int j=0; j <= b.size(); j++) {
                if(i == 0 && j == 0)
                    continue;
                List<E> l1=new ArrayList<>(a), l2=new ArrayList<>(b);
                List<E> perm=permute(l1, l2, i, j);
                perms.add(perm);
            }
        }

        for(int i=0; i <= b.size(); i++) {
            for(int j=0; j <= a.size(); j++) {
                if(i == 0 && j == 0)
                    continue;
                List<E> l1=new ArrayList<>(b), l2=new ArrayList<>(a);
                List<E> perm=permute(l1, l2, i, j);
                perms.add(perm);
            }
        }
        return perms;
    }

    protected static <E> List<E> permute(List<E> l1, List<E> l2, int remove_from_l1, int remove_from_l2) {
        List<E> retval=new ArrayList<>();
        while(!(l1.isEmpty() || l2.isEmpty())) {
            int a=remove_from_l1, b=remove_from_l2;
            while(a-- > 0 && !l1.isEmpty())
                retval.add(l1.remove(0));
            while(b-- > 0 && !l2.isEmpty())
                retval.add(l2.remove(0));
        }
        retval.addAll(l1);
        retval.addAll(l2);
        return retval;
    }

    @SafeVarargs
    public static <E> boolean checkOrder(Collection<E> perm, List<E> ... lists) {
        for(List<E> list: lists) {
            if(!perm.containsAll(list))
                return false;
            int pos=0;
            for(E el: list) {
                int index=index(perm, el);
                if(index < pos)
                    return false;
                pos=index;
            }
        }
        return true;
    }

    protected static <E> int index(Collection<E> list, E el) {
        int index=0;
        for(E element: list) {
            if(element.equals(el))
                return index;
            index++;
        }
        return -1;
    }

    /**
     * Reorders elements of an array in-place. No bounds checking is performed. Null elements are shuffled, too
     * @param array the array to be shuffled; the array will be modified
     * @param from the start index inclusive
     * @param to the end index (exclusive), must be >= from (not checked)
     * @param <T> the type of the array's elements
     */
    public static <T> void shuffle(T[] array, int from, int to) {
        if(array == null)
            return;
        for(int i=from; i < to; i++) {
            int random=(int)random(to);
            int other=random -1 + from;
            // int other=(int)(random(to)-1 + from);
            if(i != other) {
                T tmp=array[i];
                array[i]=array[other];
                array[other]=tmp;
            }
        }
    }


    public static <T> Enumeration<T> enumerate(final T[] array, int offset, final int length) {
        return new Enumeration<>() {
            final int end_pos=offset + length;
            int pos=offset;

            public boolean hasMoreElements() {
                return pos < end_pos;
            }

            public T nextElement() {
                if(pos < end_pos)
                    return array[pos++];
                throw new NoSuchElementException(String.format("pos=%d, end_pos=%d", pos, end_pos));
            }
        };
    }


    public static <T,R> Enumeration<R> enumerate(final T[] array, int offset, final int length, Function<T,R> converter) {
        return new Enumeration<>() {
            final int end_pos=offset + length;
            int pos=offset;

            public boolean hasMoreElements() {
                return pos < end_pos;
            }

            public R nextElement() {
                if(pos < end_pos)
                    return converter.apply(array[pos++]);
                throw new NoSuchElementException(String.format("pos=%d, end_pos=%d", pos, end_pos));
            }
        };
    }

    public static <T extends Number> T nonNegativeValue(T num) {
        if(num.longValue() < 0)
            throw new IllegalArgumentException(String.format("%s must not be negative", num));
        return num;
    }

    /** Returns a random value in the range [1 - range]. If range is 0, 1 will be returned. If range is negative, an
     * exception will be thrown */
    public static long random(long range) {
        return range == 0? 1 : ThreadLocalRandom.current().nextLong(range) + 1;
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
        ThreadMXBean bean=ManagementFactory.getThreadMXBean();
        ThreadInfo[] threads=bean.dumpAllThreads(true, true);
        return Stream.of(threads).map(Util::dumpThreadInfo).collect(Collectors.joining("\n"));
    }


    private static String dumpThreadInfo(ThreadInfo thread) { // copied from Infinispan
        StringBuilder sb=new StringBuilder(String.format("\"%s\" #%s prio=0 tid=0x%x nid=NA %s%n", thread.getThreadName(), thread.getThreadId(),
                                                         thread.getThreadId(), thread.getThreadState().toString().toLowerCase()));
        sb.append(String.format("   java.lang.Thread.State: %s%n", thread.getThreadState()));
        LockInfo blockedLock = thread.getLockInfo();
        StackTraceElement[] s = thread.getStackTrace();
        MonitorInfo[] monitors = thread.getLockedMonitors();
        for (int i = 0; i < s.length; i++) {
            StackTraceElement ste = s[i];
            sb.append(String.format("\tat %s\n", ste));
            if (i == 0 && blockedLock != null) {
                boolean parking = ste.isNativeMethod() && ste.getMethodName().equals("park");
                sb.append(String.format("\t- %s <0x%x> (a %s)%n", blockedState(thread, blockedLock, parking),
                                        blockedLock.getIdentityHashCode(), blockedLock.getClassName()));
            }
            if (monitors != null) {
                for (MonitorInfo monitor : monitors) {
                    if (monitor.getLockedStackDepth() == i) {
                        sb.append(String.format("\t- locked <0x%x> (a %s)%n", monitor.getIdentityHashCode(), monitor.getClassName()));
                    }
                }
            }
        }
        sb.append('\n');

        LockInfo[] synchronizers = thread.getLockedSynchronizers();
        if (synchronizers != null && synchronizers.length > 0) {
            sb.append("\n   Locked ownable synchronizers:\n");
            for (LockInfo synchronizer : synchronizers) {
                sb.append(String.format("\t- <0x%x> (a %s)%n", synchronizer.getIdentityHashCode(), synchronizer.getClassName()));
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static String blockedState(ThreadInfo thread, LockInfo blockedLock, boolean parking) {
        String state;
        if (blockedLock != null) {
            if (thread.getThreadState() == Thread.State.BLOCKED) {
                state = "waiting to lock";
            } else if (parking) {
                state = "parking to wait for";
            } else {
                state = "waiting on";
            }
        } else {
            state = null;
        }
        return state;
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


    public static String printTime(double time, TimeUnit unit) {
        switch(unit) {
            case NANOSECONDS:
                if(time < 1000) return print(time, unit);
                return printTime(time / 1000.0, TimeUnit.MICROSECONDS);
            case MICROSECONDS:
                if(time < 1000) return print(time, unit);
                return printTime(time / 1000.0, TimeUnit.MILLISECONDS);
            case MILLISECONDS:
                if(time < 1000) return print(time, unit);
                return printTime(time / 1000.0, TimeUnit.SECONDS);
            case SECONDS:
                if(time < 60) return print(time, unit);
                return printTime(time / 60.0, TimeUnit.MINUTES);
            case MINUTES:
                if(time < 60) return print(time, unit);
                return printTime(time / 60.0, TimeUnit.HOURS);
            case HOURS:
                if(time < 24) return print(time, unit);
                return printTime(time / 24.0, TimeUnit.DAYS);
            default:           return print(time, unit);
        }
    }

    public static String suffix(TimeUnit u) {
        switch(u) {
            case NANOSECONDS:  return "ns";
            case MICROSECONDS: return "us";
            case MILLISECONDS: return "ms";
            case SECONDS:      return "s";
            case MINUTES:      return "m";
            case HOURS:        return "h";
            case DAYS:         return "d";
            default:           return u.toString();
        }
    }

    public static String print(double time, TimeUnit unit) {
        return format(time, suffix(unit));
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

        int index;
        long factor=1;

        if((index=input.indexOf('k')) != -1)
            factor=1000;
        else if((index=input.indexOf("kb")) != -1)
            factor=1000;
        else if((index=input.indexOf("kib")) != -1)
            factor=1024;
        else if((index=input.indexOf('m')) != -1)
            factor=1_000_000;
        else if((index=input.indexOf("mb")) != -1)
            factor=1_000_000;
        else if((index=input.indexOf("mib")) != -1)
            factor=1_048_576;
        else if((index=input.indexOf('g')) != -1)
            factor=1_000_000_000;
        else if((index=input.indexOf("gb")) != -1)
            factor=1_000_000_000;
        else if((index=input.indexOf("gib")) != -1)
            factor=1_073_741_824;


        String str=index != -1? input.substring(0,index) : input;
        return new Tuple<>(str,factor);
    }

    public static long readDurationLong(String input, TimeUnit unit) {
        Tuple<String,Long> tuple=readDuration(input);
        BigDecimal num = new BigDecimal(tuple.getVal1());
        return unit.convert(num.multiply(new BigDecimal(tuple.getVal2())).longValue(), TimeUnit.MILLISECONDS);
    }

    private static Tuple<String,Long> readDuration(String input) {
        input=input.trim().toLowerCase();

        int index;
        long factor;

        if((index=input.indexOf("ms")) != -1)
            factor=1;
        else if((index=input.indexOf("s")) != -1)
            factor=1000;
        else if((index=input.indexOf('m')) != -1)
            factor=60_000;
        else if((index=input.indexOf("h")) != -1)
            factor=3_600_000;
        else if((index=input.indexOf('d')) != -1)
            factor=86_400_000;
        else
            factor=1;

        String str=index != -1? input.substring(0,index) : input;
        return new Tuple<>(str,factor);
    }

    /**
     * MB nowadays doesn't mean 1024 * 1024 bytes, but 1 million bytes, see <a href="http://en.wikipedia.org/wiki/Megabyte">http://en.wikipedia.org/wiki/Megabyte</a>
     * @param bytes
     */
    public static String printBytes(double bytes) {
        double tmp;

        if(bytes < 1000)
            return format(bytes, "b");
        if(bytes < 1_000_000) {
            tmp=bytes / 1000.0;
            return format(tmp, "KB");
        }
        if(bytes < 1_000_000_000) {
            tmp=bytes / 1000_000.0;
            return format(tmp, "MB");
        }
        else {
            tmp=bytes / 1_000_000_000.0;
            return format(tmp, "GB");
        }
    }

    public static String format(double val, String suffix) {
        int trailing=Math.floor(val) == val? 0 : 2;
        String fmt=String.format("%%,.%df%s", trailing, suffix);
        return String.format(fmt, val);
    }


    public static String[] components(String path,String separator) {
        if(path == null || path.isEmpty())
            return null;
        String[] tmp=path.split(separator + "+"); // multiple separators could be present
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
     * @return An array of byte buffers ({@code byte[]}).
     */
    public static byte[][] fragmentBuffer(byte[] buf,int frag_size,final int length) {
        byte[][] retval;
        byte[] fragment;
        int accumulated_size=0, tmp_size=0, num_frags, index=0;

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
        int num_frags=(int)Math.ceil(length / (double)frag_size);
        List<Range> retval=new ArrayList<>(num_frags);
        long total_size=(long)length + offset;
        int index=offset;
        int tmp_size=0;

        while(index < total_size) {
            if(index + frag_size <= total_size)
                tmp_size=frag_size;
            else
                tmp_size=(int)(total_size - index);
            Range r=new Range(index,tmp_size);
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
     * @param fragments An array of byte buffers ({@code byte[]})
     * @return A byte buffer
     */
    public static byte[] defragmentBuffer(byte[][] fragments) {
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

    public static <T> String printList(Collection<T> l) {
        return printListWithDelimiter(l, ",");
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


    public static <T> boolean contains(T key,T[] list) {
        if(list == null) return false;
        for(T tmp: list)
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
        for(short tmp: ids)
            if(tmp == id)
                return true;
        return false;
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
        if(list == null || list.isEmpty()) return null;
        int size=list.size();
        int index=(int)Util.random(size)-1;
        return list.get(index);
    }

    public static <T> T pickRandomElement(Collection<T> set) {
        if(set == null || set.isEmpty()) return null;
        int size=set.size();
        int random=(int)Util.random(size)-1;
        for(Iterator<T> it=set.iterator(); it.hasNext();) {
            T el=it.next();
            if(random-- <= 0)
                return el;
        }
        return null;
    }

    public static <T> T pickRandomElement(T[] array) {
        if(array == null) return null;
        int size=array.length;
        int index=(int)Util.random(size)-1;
        return array[index];
    }


    public static <T> T pickNext(List<T> list,T obj) {
        if(list == null || obj == null)
            return null;
        int size=list.size();
        for(int i=0; i < size; i++) {
            T tmp=list.get(i);
            if(Objects.equals(tmp, obj))
                return list.get((i + 1) % size);
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

    /** Returns the element before el. If el is the first element, returns the last element. Returns null
     * if array.length < 2 */
    public static <T> T pickPrevious(T[] array, T el) {
        if(array == null || el == null || array.length < 2)
            return null;
        for(int i=0; i < array.length; i++) {
            if(Objects.equals(el, array[i])) {
                int prev_index=i-1;
                if(prev_index < 0)
                    prev_index=array.length-1;
                return array[prev_index];
            }
        }
        return null;
    }

    public static <T> T pickPrevious(List<T> list, T el) {
        if(list == null || el == null || list.size() < 2)
            return null;
        for(int i=0; i < list.size(); i++) {
            T tmp=list.get(i);
            if(Objects.equals(el, tmp)) {
                int prev_index=i-1;
                if(prev_index < 0)
                    prev_index=list.size()-1;
                return list.get(prev_index);
            }
        }
        return null;
    }

    public static byte[] generateArray(int length) {
        byte[] array=new byte[length];
        int index=0, num=1;
        while(index + Global.INT_SIZE <= array.length) {
            Bits.writeInt(num, array, index);
            index+=Global.INT_SIZE;
            num++;
        }
        return array;
    }

    public static boolean verifyArray(byte[] array) {
        int index=0, expected_num=1;
        while(index + Global.INT_SIZE <= array.length) {
            int actual_num=Bits.readInt(array, index);
            assert expected_num == actual_num : String.format("expected %d, but got %d", expected_num, actual_num);
            index+=Global.INT_SIZE;
            expected_num++;
        }
        return true;
    }

    public static boolean verifyByteBuffer(ByteBuffer buf) {
        int index=buf.position(), expected_num=1;
        while(index + Global.INT_SIZE <= buf.limit()) {
            int actual_num=buf.getInt();
            assert expected_num == actual_num : String.format("expected %d, but got %d", expected_num, actual_num);
            index+=Global.INT_SIZE;
            expected_num++;
        }
        return true;
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
        NameCache.add(retval,name);
        return retval;
    }

    public static Object[][] createTimer() {
        return new Object[][]{
          {new TimeScheduler3()},
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



    public static <T> String print(Collection<T> objs) {
        return objs == null? "null" : objs.stream().map(Objects::toString).collect(Collectors.joining(", "));
    }

    public static String print(Object[] objs) {
        if(objs == null || objs.length == 0)
            return "";
        return objs.length == 1? (objs[0] == null? "" : objs[0].toString()) : Arrays.toString(objs);
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


    /**
     * Tries to load the class from the current thread's context class loader. If
     * not successful, tries to load the class from the current instance.
     * @param classname Desired class.
     * @param clazz     Class object used to obtain a class loader
     *                  if no context class loader is available.
     * @return Class, or null on failure.
     */
    public static Class<?> loadClass(String classname, Class<?> clazz) throws ClassNotFoundException {
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

    public static Class<? extends Protocol> loadProtocolClass(String protocol_name, Class<?> cl) throws Exception {
        String defaultProtocolName=Global.PREFIX + protocol_name;
        try {
            return (Class<? extends Protocol>) Util.loadClass(defaultProtocolName, cl);
        }
        catch(ClassNotFoundException ignored1) {
            try {
                return (Class<? extends Protocol>)Util.loadClass(protocol_name, cl);
            } catch (ClassNotFoundException ignored2) {
                throw new Exception(String.format(Util.getMessage("ProtocolLoadError"), protocol_name, defaultProtocolName));
            }
        }
    }

    @SafeVarargs
    public static Field[] getAllDeclaredFieldsWithAnnotations(final Class<?> clazz, Class<? extends Annotation>... annotations) {
        List<Field> list=new ArrayList<>(30);
        for(Class<?> curr=clazz; curr != null; curr=curr.getSuperclass()) {
            Field[] fields=curr.getDeclaredFields();
            if(fields != null) {
                for(Field field: fields) {
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



    /**
     * Applies a function against all fields and methods of a given target object which satisfy a given predicate.
     * @param obj The target object
     * @param filter The filter. Needs to be able to handle Fields and Methods (superclass: {@link AccessibleObject}).
     *               If null, all fields/methods will be selected
     * @param field_func The function to be applied to all found fields. No-op if null.
     * @param method_func The function to be applied to all found methods. No-op if null.
     */
    public static void forAllFieldsAndMethods(Object obj, Predicate<? super AccessibleObject> filter,
                                              BiConsumer<Field,Object> field_func, BiConsumer<Method,Object> method_func) {
        Objects.requireNonNull(obj, "target object cannot be null");
        if(field_func != null) {
            Stream.of(Util.getAllDeclaredFieldsWithAnnotations(obj.getClass()))
              .filter(f -> filter != null && filter.test(f)).forEach(f -> field_func.accept(f, obj));
        }
        if(method_func != null) {
            Stream.of(Util.getAllDeclaredMethodsWithAnnotations(obj.getClass()))
              .filter(m -> filter != null && filter.test(m)).peek(m -> m.setAccessible(true))
              .forEach(m -> method_func.accept(m, obj));
        }
    }


    public static String getNameFromAnnotation(AccessibleObject obj) {
        ManagedAttribute attr_annotation=obj.getAnnotation(ManagedAttribute.class);
        Property prop=obj.getAnnotation(Property.class);
        String attr_name=attr_annotation != null? attr_annotation.name() : prop != null? prop.name() : null;
        if(attr_name != null && !attr_name.trim().isEmpty())
            return attr_name.trim();
        else
            return ((Member)obj).getName();
    }


    @SafeVarargs
    public static Method[] getAllDeclaredMethodsWithAnnotations(final Class<?> clazz, Class<? extends Annotation>... annotations) {
        List<Method> list=new ArrayList<>(30);
        for(Class<?> curr=clazz; curr != null; curr=curr.getSuperclass()) {
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

    public static Field getField(final Class<?> clazz, String field_name) {
        try {
            return getField(clazz, field_name, false);
        }
        catch(NoSuchFieldException e) {
            return null;
        }
    }

    public static Field getField(final Class<?> clazz, String field_name, boolean throw_exception) throws NoSuchFieldException {
        if(clazz == null || field_name == null)
            return null;

        Field field=null;
        for(Class<?> curr=clazz; curr != null; curr=curr.getSuperclass()) {
            try {
                return curr.getDeclaredField(field_name);
            }
            catch(NoSuchFieldException ignored) {
            }
        }
        if(field == null && throw_exception)
            throw new NoSuchFieldException(String.format("%s not found in %s or superclasses", field_name, clazz.getName()));
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
                catch(Exception ignored) {
                }
            }
        }
        return null;
    }

    /** Called by the ProbeHandler impl. All args are strings. Needs to find a method where all parameter
     * types are primitive types, so the strings can be converted */
    public static Method findMethod(Class<?> target_class, String method_name, Object[] args) throws Exception {
        int len=args != null? args.length : 0;
        Method retval=null;
        Method[] methods=getAllMethods(target_class);
        for(int i=0; i < methods.length; i++) {
            Method m=methods[i];
            if(m.getName().equals(method_name)) {
                Class<?>[] parameter_types=m.getParameterTypes();
                if(parameter_types.length == len) {
                    retval=m;
                    // now check if all parameter types are primitive types:
                    boolean all_primitive=true;
                    for(Class<?> parameter_type: parameter_types) {
                        if(!isPrimitiveType(parameter_type)) {
                            all_primitive=false;
                            break;
                        }
                    }
                    if(all_primitive)
                        return m;
                }
            }
        }
        return retval;
    }

    /**
     * The method walks up the class hierarchy and returns <i>all</i> methods of this class
     * and those inherited from superclasses and superinterfaces.
     */
    public static Method[] getAllMethods(Class<?> target) {
        Class<?>    superclass = target;
        Set<Method> methods = new HashSet<>();

        while(superclass != null) {
            try {
                Method[] m = superclass.getDeclaredMethods();
                Collections.addAll(methods, m);

                // find the default methods of all interfaces (https://issues.redhat.com/browse/JGRP-2247)
                Class<?>[] interfaces=superclass.getInterfaces();
                if(interfaces != null) {
                    for(Class<?> cl: interfaces) {
                        Method[] tmp=getAllMethods(cl);
                        if(tmp != null) {
                            for(Method mm: tmp)
                                if(mm.isDefault())
                                    methods.add(mm);
                        }
                    }
                }
                superclass = superclass.getSuperclass();
            }
            catch(SecurityException e) {
                // if it runs in an applet context, it won't be able to retrieve methods from superclasses that belong
                // to the java VM, and it will raise a security exception, so we catch it here.
                superclass=null;
            }
        }

        Method[] result = new Method[methods.size()];
        int index = 0;
        for(Method m: methods)
            result[index++]=m;
        return result;
    }

    /**
     * Returns the non-null values of all fields of target annotated with @Component
     */
    public static List<Object> getComponents(Object target) {
        if(target == null)
            return null;
        Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(target.getClass(), Component.class);
        if(fields == null || fields.length == 0)
            return null;
        List<Object> components=new ArrayList<>(fields.length);
        for(Field f: fields) {
            Object comp=Util.getField(f, target);
            if(comp != null)
                components.add(comp);
        }
        return components;
    }

    /**
     * Applies a function to all fields annotated with @Component
     * @param target The target object
     * @param func The function accepting the value of the field and the component name (prefix)
     */
    public static void forAllComponents(Object target, BiConsumer<Object,String> func) {
        if(target == null)
            return;
        Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(target.getClass(), Component.class);
        if(fields == null)
            return;
        for(Field f: fields) {
            Object comp=Util.getField(f, target);
            if(comp != null) {
                Component ann=f.getAnnotation(Component.class);
                String name=ann.name();
                if(name == null || name.trim().isEmpty())
                    name=f.getName();
                func.accept(comp, name);
            }
        }
    }

    public static void forAllComponentTypes(Class<?> cl, BiConsumer<Class<?>,String> func) {
        if(cl == null)
            return;
        Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(cl, Component.class);
        if(fields == null)
            return;
        for(Field f: fields) {
            Class<?> type=f.getType();
            Component ann=f.getAnnotation(Component.class);
            String name=ann.name();
            if(name == null || name.trim().isEmpty())
                name=f.getName();
            func.accept(type, name);
        }
    }


    public static boolean isPrimitiveType(Class<?> type) {
        return type.isPrimitive()
          || type == String.class
          || type == Boolean.class
          || type == Character.class
          || type == Byte.class
          || type == Short.class
          || type == Integer.class
          || type == Long.class
          || type == Float.class
          || type == Double.class;
    }

    public static boolean isPrimitiveType(Object obj) {
        return obj == null || (obj instanceof Class<?>? TYPES.get(obj) : TYPES.get(obj.getClass())) != null;
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
                catch(Exception ignored) {
                }
            }
        }
        return null;
    }


    public static Method findMethod(Class<?> cl, String method_name, Class<?>... parameter_types) {
        for(Class<?> clazz=cl; clazz != null; clazz=clazz.getSuperclass()) {
            try {
                return clazz.getDeclaredMethod(method_name,parameter_types);
            }
            catch(Exception ignored) {
            }
        }

        // if not found, check if any of the interfaces has a (default) impl
        for(Class<?> clazz=cl; clazz != null; clazz=clazz.getSuperclass()) {
            Class<?>[] interfaces=clazz.getInterfaces();
            for(Class<?> clazz2: interfaces) {
                try {
                    return findMethod(clazz2, method_name, parameter_types); // clazz2.getDeclaredMethod(method_name, parameter_types);
                }
                catch(Exception ignored) {
                }
            }
        }
        return null;
    }


    public static Set<Class<?>> findClassesAssignableFrom(String packageName,Class<?> assignableFrom, ClassLoader cl)
      throws IOException, ClassNotFoundException {
        String path=packageName.replace('.','/');
        return findClassesAssignableFromPath(path, assignableFrom, cl);
    }


    public static Set<Class<?>> findClassesAssignableFromPath(String packagePath,Class<?> assignableFrom, ClassLoader cl)
      throws IOException, ClassNotFoundException {
        Set<Class<?>> classes=new HashSet<>();
        URL resource=cl.getResource(packagePath);
        if(resource == null)
            return classes;
        String filePath=resource.getFile();
        if(filePath == null)
            return classes;
        File f=new File(filePath);
        if(f.isDirectory()) {
            for(String file: f.list()) {
                File ff=new File(f, file);
                if(ff.isDirectory()) {
                    String dirname=packagePath + File.separator + file;
                    Set<Class<?>> clazzes=findClassesAssignableFromPath(dirname, assignableFrom, cl);
                    classes.addAll(clazzes);
                    continue;
                }
                if(file.endsWith(".class")) {
                    String name=packagePath + File.separator + file.substring(0,file.indexOf(".class"));
                    name=name.replace(File.separator, ".");
                    try {
                        Class<?> clazz=Class.forName(name);
                        if(!assignableFrom.equals(clazz) && assignableFrom.isAssignableFrom(clazz))
                            classes.add(clazz);
                    }
                    catch(ClassNotFoundException ignored) {

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


    public static InputStream getResourceAsStream(String name,Class<?> clazz) {
        return getResourceAsStream(name, clazz != null ? clazz.getClassLoader() : null);
    }

    public static InputStream getResourceAsStream(String name,ClassLoader loader) {
        InputStream retval;

        if (loader != null) {
            retval = loader.getResourceAsStream(name);
            if (retval != null)
                return retval;
        }

        try {
            loader=Thread.currentThread().getContextClassLoader();
            if(loader != null) {
                retval=loader.getResourceAsStream(name);
                if(retval != null)
                    return retval;
            }
        }
        catch(Throwable ignored) {
        }

        try {
            loader=ClassLoader.getSystemClassLoader();
            if(loader != null) {
                retval=loader.getResourceAsStream(name);
                if(retval != null)
                    return retval;
            }
        }
        catch(Throwable ignored) {
        }

        try {
            return new FileInputStream(name);
        }
        catch(FileNotFoundException e) {
        }
        return null;
    }

    public static String getChild(final Element root, String path) {
        String[] paths=path.split("\\.");
        Element current=root;
        boolean found=false;
        for(String el: paths) {
            NodeList subnodes=current.getChildNodes();
            found=false;
            for(int j=0; j < subnodes.getLength(); j++) {
                Node subnode=subnodes.item(j);
                if(subnode.getNodeType() != Node.ELEMENT_NODE)
                    continue;
                if(subnode.getNodeName().equals(el)) {
                    current=(Element)subnode;
                    found=true;
                    break;
                }
            }
        }
        return found? current.getFirstChild().getNodeValue() : null;
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


    /** e.g. "A,B,C" --> List{"A", "B", "C"} */
    public static List<String> parseCommaDelimitedStrings(String l) {
        return parseStringList(l,",");
    }

    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]". Returns a list of IpAddresses
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
            InetAddress[] resolvedAddresses=InetAddress.getAllByName(host);
            for(int i=0; i < resolvedAddresses.length; i++) {
                for(int p=port; p <= port + port_range; p++) {
                    addr=new IpAddress(resolvedAddresses[i], p);
                    retval.add(addr);
                }
            }
        }
        return new LinkedList<>(retval);
    }

    /**
     * Parses a string into a list of IpAddresses
     * @param list The list to which to add parsed elements
     * @param hosts The string with host:port pairs
     * @param unresolved_hosts A list of unresolved hosts
     * @param port_range The port range to consider
     * @return True if all hostnames resolved fine, false otherwise
     */
    public static boolean parseCommaDelimitedHostsInto(final Collection<PhysicalAddress> list,
                                                       final Collection<String> unresolved_hosts,
                                                       String hosts,int port_range, StackType stack_type) {
        StringTokenizer tok=hosts != null? new StringTokenizer(hosts,",") : null;
        boolean all_resolved=true;
        while(tok != null && tok.hasMoreTokens()) {
            String t=tok.nextToken().trim();
            String host=t.substring(0,t.indexOf('['));
            host=host.trim();
            int port=Integer.parseInt(t.substring(t.indexOf('[') + 1,t.indexOf(']')));
            try {
                InetAddress[] resolvedAddresses=InetAddress.getAllByName(host);
                for(int i=0; i < resolvedAddresses.length; i++) {
                    for(int p=port; p <= port + port_range; p++) {
                        InetAddress inet=resolvedAddresses[i];
                        boolean add=(inet == null && stack_type==StackType.Dual)
                          || (inet instanceof Inet6Address && stack_type == StackType.IPv6)
                          || (inet instanceof Inet4Address && stack_type == StackType.IPv4);
                        if(add) {
                            IpAddress addr=new IpAddress(inet, p);
                            list.add(addr);
                        }
                    }
                }
            }
            catch(UnknownHostException ex) {
                all_resolved=false;
                unresolved_hosts.add(host);
            }
        }
        return all_resolved;
    }

    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]". Returns a list of InetSocketAddress. If a hostname doesn't
     * resolve, then we'll use the hostname to create an address: new InetSocketAddress(host, port)
     */
    public static List<InetSocketAddress> parseCommaDelimitedHosts2(String hosts,int port_range) throws UnknownHostException {

        StringTokenizer tok=new StringTokenizer(hosts,",");
        String t;
        InetSocketAddress addr;
        Set<InetSocketAddress> retval=new HashSet<>();

        while(tok.hasMoreTokens()) {
            t=tok.nextToken().trim();
            String host=t.substring(0,t.indexOf('['));
            host=host.trim();
            int port=Integer.parseInt(t.substring(t.indexOf('[') + 1,t.indexOf(']')));

            InetAddress[] resolvedAddresses=null;
            try {
                resolvedAddresses=InetAddress.getAllByName(host);
            }
            catch(Exception ex) {
            }
            if(resolvedAddresses != null) {
                for(int i=0; i < resolvedAddresses.length; i++) {
                    for(int p=port; p <= port + port_range; p++) {
                        addr=new InetSocketAddress(resolvedAddresses[i],p);
                        retval.add(addr);
                    }
                }
            }
            else {
                for(int p=port; p <= port + port_range; p++) {
                    addr=new InetSocketAddress(host,p);
                    retval.add(addr);
                }
            }
        }
        return new LinkedList<>(retval);
    }

    /** Parses a host:IP string into an InetSocketAddress */
    public static InetSocketAddress parseHost(String s) {
        s=s.trim();
        int index=s.lastIndexOf(":");
        String host=index > -1? s.substring(0, index) : s;
        String port_str=index > -1? s.substring(index+1) : "0";
        return new InetSocketAddress(host, Integer.parseInt(port_str));
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

    public static String[] parseStringArray(String s, String separator) {
        if(s == null || s.isEmpty())
            return new String[]{};
        String[] list=s.split(separator != null? separator : ",");
        List<String> tmp=Stream.of(list).map(String::trim).filter(Objects::nonNull).collect(Collectors.toList());
        String[] retval=new String[tmp.size()];
        for(int i=0; i < tmp.size(); i++)
            retval[i]=tmp.get(i);
        return retval;
    }

    /**
     * Reads a line of text.  A line is considered to be terminated by any one of a line feed ('\n'), a carriage
     * return ('\r'), or a carriage return followed immediately by a linefeed.
     * @return A String containing the contents of the line, not including any line-termination characters, or
     * null if the end of the stream has been reached
     * @throws IOException If an I/O error occurs
     */
    public static String readLine(InputStream in) throws IOException {
        StringBuilder sb=new StringBuilder(35);
        int ch;

        while(true) {
            ch=in.read();
            if(ch == -1)
                return sb.toString();
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

    /** Reads all bytes fro, an input stream, until eof (-1) is reached */
    public static ByteArray readBytes(InputStream in) throws IOException {
        byte[] retval=new byte[in.available()];
        int index=0;
        while(true) {
            int ch=in.read();
            switch(ch) {
                //case '\r':
                  //  break;
                case '\n':
                case -1:
                    return new ByteArray(retval, 0, index);
                default:
                    if(index+1 >= retval.length)
                        retval=Arrays.copyOf(retval, retval.length+5);
                    retval[index++]=(byte)ch;
                    break;
            }
        }
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
        return interfaces == null? "null" : interfaces.stream().map(NetworkInterface::getName).collect(Collectors.joining(", "));
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
     * @see JChannel#startFlush(List,boolean)
     */
    public static boolean startFlush(JChannel c,List<Address> flushParticipants,
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
     * @see JChannel#startFlush(List,boolean)
     */
    public static boolean startFlush(JChannel c,List<Address> flushParticipants) {
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
     * @see JChannel#startFlush(boolean)
     */
    public static boolean startFlush(JChannel c,int numberOfAttempts,long randomSleepTimeoutFloor,long randomSleepTimeoutCeiling) {
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
     * @see JChannel#startFlush(boolean)
     */
    public static boolean startFlush(JChannel c) {
        return startFlush(c,4,1000,5000);
    }

    public static String shortName(InetAddress hostname) {
        if(hostname == null) return null;
        return hostname.getHostAddress();
    }

    public static String generateLocalName() {
        String retval=null;
        try {
            retval=shortName(InetAddress.getLocalHost().getHostName());
        }
        catch(Throwable ignored) {
        }
        if(retval == null) {
            try {
                retval=shortName(InetAddress.getByName(null).getHostName());
            }
            catch(Throwable e) {
                retval="localhost";
            }
        }

        long counter=Util.random((long)Short.MAX_VALUE * 2);
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

    public static ServerSocket createServerSocket(SocketFactory factory, String service_name, InetAddress bind_addr,
                                                  int start_port, int end_port, int recv_buf_size) throws Exception {
        ServerSocket ret=null;
        try {
            ret=factory.createServerSocket(service_name);
            if(recv_buf_size > 0)
                ret.setReceiveBufferSize(recv_buf_size); // https://issues.redhat.com/browse/JGRP-2504
            Util.bind(ret, bind_addr, start_port, end_port);
            return ret;
        }
        catch(Exception e) {
            Util.close(ret);
            throw e; // return null;
        }
    }




    public static void bind(ServerSocket srv_sock, InetAddress bind_addr,
                            int start_port, int end_port) throws Exception {
        int original_start_port=start_port;

        while(true) {
            try {
                InetSocketAddress sock_addr=new InetSocketAddress(bind_addr, start_port);
                srv_sock.bind(sock_addr);
                break;
            }
            catch(SocketException bind_ex) {
                if(start_port == end_port)
                    throw new BindException("No available port to bind to in range [" + original_start_port + " .. " + end_port + "]");
                if(bind_addr != null && !bind_addr.isLoopbackAddress() && !bind_addr.isAnyLocalAddress()) {
                    NetworkInterface nic=NetworkInterface.getByInetAddress(bind_addr);
                    if(nic == null)
                        throw new BindException("bind_addr " + bind_addr + " is not a valid interface: " + bind_ex);
                }
                start_port++;
            }
        }
    }


    public static void bind(Socket sock, InetAddress bind_addr, int start_port, int end_port) throws Exception {
        int original_start_port=start_port;

        while(true) {
            try {
                InetSocketAddress sock_addr=new InetSocketAddress(bind_addr, start_port);
                sock.bind(sock_addr);
                break;
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


    public static ServerSocketChannel createServerSocketChannel(SocketFactory factory,String service_name, InetAddress bind_addr,
                                                                int start_port, int end_port, int recv_buf_size) throws Exception {
        int original_start_port=start_port;
        ServerSocketChannel ch=null;
        while(true) {
            try {
                Util.close(ch);
                ch=factory.createServerSocketChannel(service_name);
                if(recv_buf_size > 0)
                    ch.setOption(StandardSocketOptions.SO_RCVBUF, recv_buf_size);
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
     * Method used by PropertyConverters.BindInterface to check that a bind_addr is consistent with a specified interface
     * <p/>
     * Idea:
     * 1. We are passed a bind_addr, which may be null
     * 2. If non-null, check that bind_addr is on bind_interface - if not, throw exception,
     *    otherwise, return the original bind_addr
     * 3. If null, get first non-loopback address on bind_interface, using stack preference to get the IP version.
     *    If no non-loopback address, then just return null (i.e. bind_interface did not influence the decision).
     */
    public static InetAddress validateBindAddressFromInterface(InetAddress bind_addr,
                                                               String bind_interface_str, StackType ip_version) throws UnknownHostException, SocketException {
        NetworkInterface bind_intf=null;

        if(bind_addr != null && bind_addr.isLoopbackAddress())
            return bind_addr;

        // if bind_interface_str is null, or empty, no constraint on bind_addr
        if(bind_interface_str == null || bind_interface_str.trim().isEmpty())
            return bind_addr;

        // if bind_interface_str specified, get interface and check that it has correct version
        bind_intf=Util.getByName(bind_interface_str); // NetworkInterface.getByName(bind_interface_str);
        if(bind_intf != null) {
            // check that the interface supports the IP version
            boolean supportsVersion=interfaceHasIPAddresses(bind_intf,ip_version);
            if(!supportsVersion)
                throw new IllegalArgumentException("bind_interface " + bind_interface_str + " has incorrect IP version");
        }
        else
            throw new UnknownHostException("network interface " + bind_interface_str + " not found");

        // 3. intf and bind_addr are both are specified, bind_addr needs to be on intf
        if(bind_addr != null) {
            boolean hasAddress=false;
            Enumeration<InetAddress> addresses=bind_intf.getInetAddresses();

            while(addresses != null && addresses.hasMoreElements()) {
                // get the next InetAddress for the current interface
                InetAddress address=addresses.nextElement();
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
        else
            bind_addr=getAddress(bind_intf, AddressScope.NON_LOOPBACK, ip_version);

        //https://issues.redhat.com/browse/JGRP-739
        //check all bind_address against NetworkInterface.getByInetAddress() to see if it exists on the machine
        //in some Linux setups NetworkInterface.getByInetAddress(InetAddress.getLocalHost()) returns null, so skip
        //the check in that case
        if(bind_addr != null && NetworkInterface.getByInetAddress(bind_addr) == null)
            throw new UnknownHostException("Invalid bind address " + bind_addr);

        // if bind_addr == null, we have tried to obtain a bind_addr but were not successful
        // in such a case, return the original value of null so the default will be applied
        return bind_addr;
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

    public static boolean checkForMac() {return checkForPresence("os.name", "mac", "macosx", "osx", "darwin");}

    private static boolean checkForPresence(String key, String ...values) {
        if(values == null)
            return false;
        for(String val: values)
            if(checkForPresence(key, val))
                return true;
        return false;
    }

    private static boolean checkForPresence(String key,String value) {
        try {
            String tmp=SecurityActions.getProperty(key);
            return tmp != null && tmp.trim().toLowerCase().startsWith(value);
        }
        catch(Throwable t) {
            return false;
        }
    }

    private static boolean contains(String key,String value) {
        try {
            String tmp=SecurityActions.getProperty(key);
            return tmp != null && tmp.trim().toLowerCase().contains(value.trim().toLowerCase());
        }
        catch(Throwable t) {
            return false;
        }
    }


    /** IP related utilities */
    public static InetAddress getLoopback(StackType ip_version) throws UnknownHostException {
        if(ip_version == StackType.IPv6)
            return InetAddress.getByName("::1");
        return InetAddress.getLoopbackAddress();
    }

    public static InetAddress getLoopback() throws UnknownHostException {
        return getLoopback(Util.getIpStackType());
    }


    /** Returns the first non-loopback address on any interface on the current host */
    public static InetAddress getNonLoopbackAddress() throws SocketException {
        return getAddress(AddressScope.NON_LOOPBACK, Util.getIpStackType());
    }

    public static InetAddress getNonLoopbackAddress(StackType ip_version) throws SocketException {
        return getAddress(AddressScope.NON_LOOPBACK, ip_version);
    }

    /** Finds a network interface or sub-interface with the given name */
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

    /**
     * Finds an address given a symbolic name. Parameter ip_version has precedence over system props such as
     * java.net.preferIPv4Stack or java.net.preferIPv6Addresses
     * @param host The symbolic nbame of the host
     * @param ip_version The IP version, e.g. {@link StackType#IPv4} or {@link StackType#IPv6}
     * @return The resolved address
     * @throws UnknownHostException Thrown if host cannot be resolved to an InetAddress
     */
    public static InetAddress getByName(String host, StackType ip_version) throws UnknownHostException {
        if(ip_version == null || ip_version == StackType.Dual)
            return InetAddress.getByName(host);
        Class<?> clazz=ip_version == StackType.IPv6? Inet6Address.class : Inet4Address.class;
        InetAddress[] addrs=InetAddress.getAllByName(host); // always returns a non-null at least 1-element array
        return Stream.of(addrs).filter(a -> a != null && a.getClass() == clazz)
          .findFirst().orElse(null);
    }

    public static InetAddress getAddress(String value, StackType ip_version) throws Exception {
        try {
            AddressScope addr_scope=AddressScope.valueOf(value.toUpperCase());
            return Util.getAddress(addr_scope, ip_version);
        }
        catch(Throwable ignored) {
        }

        if(value.startsWith("match"))
            return Util.getAddressByPatternMatch(value, ip_version);
        if(value.startsWith("custom:"))
            return Util.getAddressByCustomCode(value.substring("custom:".length()));
        return Util.getByName(value, ip_version);
    }


    /**
     * Returns the first address on any interface which satisfies scope and ip_version. If ip_version is Dual, then
     * IPv4 addresses are favored
     */
    public static InetAddress getAddress(AddressScope scope, StackType ip_version) throws SocketException {
        Collection<InetAddress> all_addrs=getAllAvailableAddresses(scope != null? a -> match(a, scope) : null);
        if(scope != null) {
            switch(ip_version) {
                case IPv6:
                    return all_addrs.stream().filter(a -> a instanceof Inet6Address).findFirst().orElse(null);
                case IPv4: case Dual:
                    return all_addrs.stream().filter(a -> a instanceof Inet4Address).findFirst().orElse(null);
            }
        }
        return all_addrs.stream().findFirst().orElse(null);
    }

    /**
     * Returns the first address on the given interface on the current host, which satisfies scope
     * @param intf the interface to be checked
     */
    public static InetAddress getAddress(NetworkInterface intf, AddressScope scope, StackType ip_version) {
        InetAddress first=null;
        for(Enumeration<InetAddress> addresses=intf.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress addr=addresses.nextElement();
            if(scope == null || match(addr,scope)) {
                if((addr instanceof Inet4Address && (ip_version == StackType.IPv4 || ip_version == StackType.Dual)) ||
                  (addr instanceof Inet6Address && ip_version == StackType.IPv6))
                    return addr;
                if(first == null)
                    first=addr;
            }
        }
        return ip_version == StackType.Dual? first : null;
    }

    public static boolean match(InetAddress addr, AddressScope scope) {
        if(scope == null)
            return true;
        switch(scope) {
            case GLOBAL:
                return !addr.isLoopbackAddress() && !addr.isLinkLocalAddress() && !addr.isSiteLocalAddress();
            case SITE_LOCAL:
                return addr.isSiteLocalAddress();
            case LINK_LOCAL:
                return addr.isLinkLocalAddress();
            case LOOPBACK:
                return addr.isLoopbackAddress();
            case NON_LOOPBACK:
                return !addr.isLoopbackAddress();
            default:
                throw new IllegalArgumentException("scope " + scope + " is unknown");
        }
    }

    /**
     * Returns a valid interface address based on a pattern. Iterates over all interfaces that are up and
     * returns the first match, based on the address or interface name
     * @param pattern Can be "match-addr:<pattern></pattern>" or "match-interface:<pattern></pattern>". Example:<p/>
     *                match-addr:192.168.*
     * @return InetAddress or null if not found
     */
    public static InetAddress getAddressByPatternMatch(String pattern, StackType ip_version) throws Exception {
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
        Enumeration<NetworkInterface> intfs=NetworkInterface.getNetworkInterfaces();
        while(intfs.hasMoreElements()) {
            NetworkInterface intf=intfs.nextElement();
            try {
                if(!isUp(intf))
                    continue;
                switch(type) {
                    case 1: // match by interface name
                        String interface_name=intf.getName();
                        Matcher matcher=pat.matcher(interface_name);
                        if(matcher.matches())
                            return getAddress(intf,null, ip_version);
                        break;
                    case 2: // match by host address
                    case 3: // match by host name
                        InetAddress first=null;
                        for(Enumeration<InetAddress> en=intf.getInetAddresses(); en.hasMoreElements();) {
                            InetAddress addr=en.nextElement();
                            String name=type == 3? addr.getHostName() : addr.getHostAddress();
                            matcher=pat.matcher(name);
                            if(matcher.matches()) {
                                if((addr instanceof Inet4Address && (ip_version == StackType.IPv4 || ip_version == StackType.Dual)) ||
                                  (addr instanceof Inet6Address && ip_version == StackType.IPv6))
                                    return addr;
                                if(first == null)
                                    first=addr;
                            }
                        }
                        if(first != null)
                            return first;
                        break;
                }
            }
            catch(SocketException ignored) {
            }
        }
        return null;
    }
    public static InetAddress getAddressByCustomCode(String value) throws Exception {
        Class<Supplier<InetAddress>> clazz=(Class<Supplier<InetAddress>>)Util.loadClass(value, (ClassLoader)null);
        Supplier<InetAddress> supplier=clazz.getDeclaredConstructor().newInstance();
        return supplier.get();
    }

    /** Always returns true unless there is a socket exception - will be removed when GraalVM issue
     * https://github.com/oracle/graal/pull/1076 has been fixed */
    public static boolean isUp(NetworkInterface ni) throws SocketException {
        try {
            return ni.isUp();
        }
        catch(SocketException sock_ex) {
            throw sock_ex;
        }
        catch(Throwable t) {
            return true;
        }
    }



    /** A function to check if an interface supports an IP version (i.e. has addresses defined for that IP version) */
    public static boolean interfaceHasIPAddresses(NetworkInterface intf,StackType ip_version) throws UnknownHostException {
        boolean supportsVersion=false;
        if(intf != null) {
            // get all the InetAddresses defined on the interface
            Enumeration<InetAddress> addresses=intf.getInetAddresses();
            while(addresses != null && addresses.hasMoreElements()) {
                // get the next InetAddress for the current interface
                InetAddress address=addresses.nextElement();

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

    /**
     * Tries to determine the type of IP stack from the available interfaces and their addresses and from the
     * system properties (java.net.preferIPv4Stack and java.net.preferIPv6Addresses)
     * @return StackType.IPv4 for an IPv4 only stack, StackType.IPv6 for an IPv6 only stack, and StackType.Unknown
     * if the type cannot be detected
     */
    private static StackType _getIpStackType() {
        Collection<InetAddress> all_addresses=getAllAvailableAddresses();
        for(InetAddress addr: all_addresses) {
            if(addr instanceof Inet4Address)
                ipv4_stack_available=true;
            else if(addr instanceof Inet6Address)
                ipv6_stack_available=true;
            if(ipv4_stack_available && ipv6_stack_available)
                break;
        }

        // if only IPv4 stack available
        if(ipv4_stack_available && !ipv6_stack_available)
            return StackType.IPv4;
        // if only IPv6 stack available
        if(ipv6_stack_available && !ipv4_stack_available)
            return StackType.IPv6;
        // If dual stack or no stack available: get the System property which records user pref for a stack
        // If both java.net.preferIPv4Stack _and_ java.net.preferIPv6Addresses are set, prefer IPv4 (like the JDK does)
        return Boolean.getBoolean(Global.IPv4)? StackType.IPv4
          : Boolean.getBoolean(Global.IPv6)? StackType.IPv6 : StackType.Dual;
    }


    public static boolean isStackAvailable(boolean ipv4) {
        Collection<InetAddress> all_addrs=getAllAvailableAddresses();
        for(InetAddress addr : all_addrs)
            if(ipv4 && addr instanceof Inet4Address || (!ipv4 && addr instanceof Inet6Address))
                return true;
        return false;
    }


    public static List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
        List<NetworkInterface> cached_interfaces=CACHED_INTERFACES;
        if(cached_interfaces != null)
            return cached_interfaces;

        List<NetworkInterface> retval=new ArrayList<>(10);
        for(Enumeration<NetworkInterface> en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
            NetworkInterface intf=en.nextElement();
            retval.add(intf);
            for(Enumeration<NetworkInterface> subs=intf.getSubInterfaces(); subs.hasMoreElements();) {
                NetworkInterface sub=subs.nextElement();
                if(sub != null)
                    retval.add(sub);
            }
        }
        return CACHED_INTERFACES=retval;
    }

    public static void resetCacheAddresses(boolean reset_interfaces, boolean reset_addresses) {
        if(reset_interfaces)
            CACHED_INTERFACES=null;
        if(reset_addresses)
            CACHED_ADDRESSES=null;
    }

    /** This is a workaround for use within android. Sometimes the standard java methods do not return all the addresses. 
     * This will allow for setting in android via the conectivity manager without needing to pass in the context.
     * Also will allow android to update as connectivity changes.
     */
    public static void setCacheAddresses(List<NetworkInterface> interfaces, List<InetAddress> addresses) {
        CACHED_INTERFACES=interfaces;
        CACHED_ADDRESSES=addresses;
    }
    /** Returns all addresses of all interfaces (that are up) that satisfy a given filter (ignored if null) */
    public static Collection<InetAddress> getAllAvailableAddresses(Predicate<InetAddress> filter) {
        Collection<InetAddress> cached_addresses=getAllAvailableAddresses();
        assert cached_addresses != null;
        return filter == null ?
                cached_addresses :
                cached_addresses.stream().filter(filter).collect(Collectors.toList());
    }

    private static synchronized Collection<InetAddress> getAllAvailableAddresses() {
        if(CACHED_ADDRESSES != null)
            return CACHED_ADDRESSES;
        Set<InetAddress> addresses=new HashSet<>();
        try {
            List<NetworkInterface> interfaces=getAllAvailableInterfaces();
            for(NetworkInterface intf: interfaces) {
                if(!isUp(intf)  /*!intf.isUp()*/)
                    continue;
                Enumeration<InetAddress> inetAddresses=intf.getInetAddresses();
                while(inetAddresses.hasMoreElements()) {
                    addresses.add(inetAddresses.nextElement());
                }
            }
        }
        catch(SocketException e) {
        }
        // immutable list
        return CACHED_ADDRESSES=List.copyOf(addresses);
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
        StringBuilder sb=new StringBuilder();
        if(prot_name != null)
            sb.append('[').append(prot_name).append("] ");
        sb.append(bind_addr).append(" is not a valid address on any local network interface");
        throw new BindException(sb.toString());
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
                        tmp=SecurityActions.getProperty(prop);
                        if(tmp != null)
                            return tmp;
                    }
                    catch(SecurityException ignored) {
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
        Address coord=view.getCoord();
        return local_addr.equals(coord);
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
     * Replaces variables of ${var:default} with System.getProperty(var, default). If no variables are found, returns
     * the same string, otherwise a copy of the string with variables substituted
     * @param val
     * @return A string with vars replaced, or the same string if no vars found
     */
    public static String substituteVariable(String val) {
        return substituteVariable(val, null);
    }

    /**
     * Replaces variables of ${var:default} with Properties then uses System.getProperty(var, default) if the value was
     * not found. If no variables are found, returns the same string, otherwise a copy of the string with variables
     * substituted
     * @param val
     * @param p
     * @return A string with vars replaced, or the same string if no vars found
     */
    public static String substituteVariableOld(String val, Properties p) {
        if(val == null)
            return val;
        String retval=val, prev;

        while(retval.contains("${")) { // handle multiple variables in val
            prev=retval;
            retval=_substituteVar(retval, p);
            if(retval.equals(prev))
                break;
        }
        return retval;
    }

    /**
     * Replaces variables in a string.
     * @param input The input string
     * @param p A Properties hashmap, may be null
     * @return The string with replaced variables, might be the same string as the input string if no substitution took place.
     */
    public static String substituteVariable(String input, Properties p) {
        if(input == null)
            return input;
        StringBuilder sb=new StringBuilder(input.length()), cache=null;
        for(int i=0; i < input.length(); i++) {
            char ch=input.charAt(i);
            switch(ch) {
                case '$':
                    char next=nextChar(input, i+1);
                    if(next == 0) { // end of string
                        sb.append(ch);
                        continue; // will terminate
                    }
                    if(next == '{') { // found '${'
                        i++;
                        if(cache != null) // we've already encountered a '${', but it is ignored
                            sb.append(cache);
                        cache=new StringBuilder(input.length()).append(ch).append(next);
                    }
                    else
                        sb.append(ch); // .append(next);
                    break;
                case '}':
                    if(cache != null) {
                        String val=cache.substring(2);
                        String tmp=getProperty(val, p);
                        if(tmp != null)
                            sb.append(tmp);
                        cache=null;
                    }
                    else
                        sb.append(ch);
                    break;
                default:
                    if(cache != null)
                        cache.append(ch);
                    else
                        sb.append(ch);
                    break;
            }
        }
        if(cache != null)
            sb.append(cache);
        return sb.length() == 0? null : sb.toString();
    }

    protected static char nextChar(String s, int index) {
        return index >= s.length()? 0 : s.charAt(index);
    }

    private static String _substituteVar(String val, Properties p) {
        int start_index, end_index;
        start_index=val.indexOf("${");
        if(start_index == -1 || val.contains("\\${"))
            return val;
        end_index=val.indexOf("}",start_index + 2);
        if(end_index == -1)
            throw new IllegalArgumentException("missing \"}\" in " + val);

        String tmp=getProperty(val.substring(start_index + 2,end_index),p);
        if(tmp == null)
            return val;
        StringBuilder sb = new StringBuilder();
        sb.append(val, 0, start_index).append(tmp).append(val.substring(end_index + 1));
        return sb.toString();
    }

    public static String getProperty(String s, Properties p) {
        String var, default_val, retval=null;
        int index=s.indexOf(':');
        if(index >= 0) {
            var=s.substring(0,index);
            default_val=s.substring(index + 1);
            if(default_val != null && !default_val.isEmpty())
                default_val=default_val.trim();
            retval=_getProperty(var,default_val,p);
        }
        else {
            var=s;
            retval=_getProperty(var,null,p);
        }
        return retval;
    }

    public static String getProperty(String s) {
        return getProperty(s, null);
    }

    /**
     * Parses a var which might be comma delimited, e.g. bla,foo:1000: if 'bla' is set, return its value. Else,
     * if 'foo' is set, return its value, else return "1000"
     * @param var
     * @param default_value
     * @return
     */
    private static String _getProperty(String var,String default_value, Properties p) {
        if(var == null)
            return null;
        List<String> list=parseCommaDelimitedStrings(var);
        if(list == null || list.isEmpty()) {
            list=new ArrayList<>(1);
            list.add(var);
        }
        String retval;
        for(String prop : list) {
            try {
                if(p != null && (retval=p.getProperty(prop)) != null)
                    return retval;
                if((retval=SecurityActions.getProperty(prop)) != null)
                    return retval;
                if((retval=SecurityActions.getEnv(prop)) != null)
                    return retval;
            }
            catch(Throwable ignored) {
            }
        }
        return default_value;
    }


    /** Converts a method name to an attribute name, e.g. getFooBar() --> foo_bar, isFlag --> flag */
    public static String methodNameToAttributeName(final String methodName) {
        String name=methodName;
        if((methodName.startsWith("get") || methodName.startsWith("set")) && methodName.length() > 3)
            name=methodName.substring(3);
        else if(methodName.startsWith("is") && methodName.length() > 2)
            name=methodName.substring(2);

        // Pattern p=Pattern.compile("[A-Z]+");
        Matcher m=METHOD_NAME_TO_ATTR_NAME_PATTERN.matcher(name);
        // Fix android platform NoSuchMethodException: Matcher.appendReplacement(StringBuffer, String);
        //noinspection StringBufferMayBeStringBuilder
        StringBuffer sb=new StringBuffer();
        int start=0, end=0;
        while(m.find()) {
            start=m.start();
            end=m.end();
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
        // m.appendTail(sb); // https://issues.redhat.com/browse/JGRP-2670
        sb.append(name, end, name.length());

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
            // Fix android platform NoSuchMethodException: Matcher.appendReplacement(StringBuffer, String);
            //noinspection StringBufferMayBeStringBuilder
            StringBuffer sb=new StringBuffer();
            int end=0;
            while(m.find()) {
                end=m.end();
                m.appendReplacement(sb,attr_name.substring(m.end() - 1,m.end()).toUpperCase());
            }
            // m.appendTail(sb); // https://issues.redhat.com/browse/JGRP-2670
            sb.append(attr_name, end, attr_name.length());
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


    public static Map<String,List<Address>> getSites(View bridge_view, String excluding_site) {
        Map<String,List<Address>> m=new HashMap<>();
        if(bridge_view == null)
            return m;
        for(Address a: bridge_view) {
            if(a instanceof SiteUUID) {
                String sitename=((SiteUUID)a).getSite();
                if(sitename == null || sitename.equals(excluding_site))
                    continue;
                List<Address> site_addrs=m.computeIfAbsent(sitename, s -> new ArrayList<>());
                site_addrs.add(a);
            }
        }
        return m;
    }

    /**
     * Returns the sites of the view (all addresses are SiteUUIDs) minus the given site, Example:
     * bridge_view=A:net1,B:net1,X:net2,Y:net2, excluding_site=net1 -> ["net2"]
     * @param bridge_view
     * @param excluding_site
     * @return the sites of members who are _not_ in excluding_site; each site is returned onlt once
     */
    public static Collection<String> otherSites(View bridge_view, String excluding_site) {
        if(bridge_view == null)
            return Collections.emptySet();
        Set<String> ret=new HashSet<>(bridge_view.size());
        Address[] members=bridge_view.getMembersRaw();
        for(Address addr: members) {
            if(addr instanceof SiteUUID) {
                String site=((SiteUUID)addr).getSite();
                if(site != null && !site.equals(excluding_site))
                    ret.add(site);
            }
        }
        return ret;
    }

    public static String utcNow() {
        return UTF_FORMAT.format(LocalDateTime.now(ZoneOffset.UTC));
    }

    public static String utcEpoch(long milliseconds) {
        return UTF_FORMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(milliseconds), ZoneOffset.UTC));
    }

}
