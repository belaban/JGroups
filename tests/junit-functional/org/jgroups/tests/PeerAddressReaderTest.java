package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.blocks.cs.NioConnection;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.FastArray;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.jgroups.blocks.cs.Connection.cookie;

/**
 * Tests {@link org.jgroups.blocks.cs.NioConnection.PeerAddressReader}
 * @author Bela Ban
 * @since  5.5.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="createAddress")
public class PeerAddressReaderTest {
    protected NioConnection.PeerAddressReader reader;
    protected MockSocketChannel channel;
    protected Address ipv4, ipv6;

    @DataProvider
    protected Object[][] createAddress() {
        return new Object[][] {
          {ipv4},
          {ipv6}
        };
    }

    @BeforeClass
    protected void createAddresses() throws UnknownHostException {
        ipv4=new IpAddress(Util.getLoopback(StackType.IPv4), 8000);
        ipv6=new IpAddress(Util.getLoopback(StackType.IPv6), 8000);
    }

    @BeforeMethod
    protected void setup() {
        reader=new NioConnection.PeerAddressReader(false);
        channel=new MockSocketChannel();
    }

    public void testFullRead(Address addr) throws IOException {
        byte[] bytes_to_read=create(addr);
        channel.bytesToRead(bytes_to_read);
        Address tmp=reader.readPeerAddress(channel);
        assert addr.equals(tmp);
    }

    public void testPartialRead(Address addr) throws IOException {
        byte[] bytes_to_read=create(addr);
        List<byte[]> fragments=new FastArray<>();
        fragments.add(Arrays.copyOfRange(bytes_to_read, 0, 8));
        fragments.add(Arrays.copyOfRange(bytes_to_read, 8, 9));
        fragments.add(Arrays.copyOfRange(bytes_to_read, 9, 10));
        fragments.add(Arrays.copyOfRange(bytes_to_read, 10, 14));
        fragments.add(Arrays.copyOfRange(bytes_to_read, 14, bytes_to_read.length));
        for(int i=0; i < fragments.size(); i++) {
            byte[] frag=fragments.get(i);
            channel.addBytesToRead(frag);
            Address tmp=reader.readPeerAddress(channel);
            if(i == fragments.size()-1)
                assert Objects.equals(tmp, addr);
            else
                assert tmp == null;
        }
    }

    public void testReadFromClosedChannel(Address addr) throws IOException {
        byte[] bytes_to_read=create(addr);
        channel.close();
        channel.bytesToRead(bytes_to_read);
        Address tmp=reader.readPeerAddress(channel);
        assert tmp ==null;
    }

    public void testReadAndClose(Address addr) throws IOException {
        byte[] bytes_to_read=create(addr);
        channel.addBytesToRead(Arrays.copyOfRange(bytes_to_read, 0, 8));
        Address tmp=reader.readPeerAddress(channel);
        assert tmp ==null;
        channel.addBytesToRead(Arrays.copyOfRange(bytes_to_read, 8,14));
        channel.close();
        tmp=reader.readPeerAddress(channel);
        assert tmp ==null;
    }

    protected static byte[] create(Address addr) throws IOException {
        int addr_size=addr.serializedSize();
        int expected_size=cookie.length + Global.SHORT_SIZE*2 + addr_size;
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size);
        out.write(cookie, 0, cookie.length);
        out.writeShort(Version.version);
        out.writeShort(addr_size); // address size
        addr.writeTo(out);
        return out.buffer();
    }

}
