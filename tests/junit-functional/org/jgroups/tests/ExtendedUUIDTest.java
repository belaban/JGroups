package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.FlagsUUID;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link ExtendedUUID}
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class ExtendedUUIDTest {

    public void testCreation() {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A").setFlag(RELAY2.site_master_flag);
        System.out.println("uuid = " + uuid);
    }

    public void testFlags() {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A").setFlag(RELAY2.site_master_flag).setFlag((short)2).setFlag((short)4);
        System.out.println("uuid = " + uuid);
        assert uuid.isFlagSet(RELAY2.site_master_flag);
        assert uuid.isFlagSet((short)2);
        assert uuid.isFlagSet((short)4);
        uuid.clearFlag((short)2);
        assert !uuid.isFlagSet((short)2);
    }

    public void testFlags2() throws Exception {
        FlagsUUID uuid=FlagsUUID.randomUUID("A").setFlag(RELAY2.site_master_flag).setFlag((short)2).setFlag((short)4);
        System.out.println("uuid = " + uuid);
        assert uuid.isFlagSet(RELAY2.site_master_flag);
        assert uuid.isFlagSet((short)2);
        assert uuid.isFlagSet((short)4);
        uuid.clearFlag((short)2);
        assert !uuid.isFlagSet((short)2);

        byte[] buf=Util.streamableToByteBuffer(uuid);
        FlagsUUID uuid2=Util.streamableFromByteBuffer(FlagsUUID::new, buf, 0, buf.length);
        assert uuid.equals(uuid2);
    }


    public void testPut() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A").put("name", Util.objectToByteBuffer("Bela"))
          .put("age",Util.objectToByteBuffer(49)).put("bool",Util.objectToByteBuffer(true));
        assert uuid.keyExists("name");
        assert uuid.keyExists("bool");
        byte[] val=uuid.get("age");
        assert Util.objectFromByteBuffer(val).equals(49);
    }

    public void testAddContents() throws Exception {
        ExtendedUUID a=ExtendedUUID.randomUUID("A").setFlag((short)1);
        a.put("name",Util.stringToBytes("Bela")).put("age",Util.objectToByteBuffer(49)).put("bool",Util.objectToByteBuffer(true));
        ExtendedUUID b=ExtendedUUID.randomUUID("B").setFlag((short)2).setFlag((short)4);
        b.put("one",null).put("two",Util.stringToBytes("two")).put("name", Util.stringToBytes("Michelle"));
        a.addContents(b);
        for(short flag: Arrays.asList((short)1,(short)2, (short)4))
            assert a.isFlagSet(flag);
        for(String key: Arrays.asList("name", "age", "bool", "one", "two"))
            assert a.keyExists(key);
        assert Util.bytesToString(a.get("two")).equals("two");
        assert Util.bytesToString(a.get("name")).equals("Bela");
    }


    public void testRemove() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A");
        byte[] val=Util.objectToByteBuffer("tmp");
        for(int i=1; i <= 10; i++)
            uuid.put(String.valueOf(i), val);

        byte[] tmp=uuid.remove(String.valueOf(5));
        assert tmp != null && Arrays.equals(tmp, val);

        tmp=uuid.get(String.valueOf(5));
        assert tmp == null;

        tmp=uuid.get(String.valueOf(7));
        assert tmp != null && Arrays.equals(tmp, val);

        uuid.remove(String.valueOf(7));
        assert uuid.keyExists(String.valueOf(8));
    }

    public void testResize() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A");
        byte[] val=Util.objectToByteBuffer("tmp");
        for(int i=1000; i <= 1005; i++) {
            uuid.put(String.valueOf(i), val);
            assert uuid.keyExists(String.valueOf(i));
        }
    }

    public void testResizeBeyond255() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A");
        byte[] val=Util.objectToByteBuffer("tmp");
        for(int i=1; i <= 0xff; i++)
            uuid.put(String.valueOf(i), val);

        try {
            uuid.put(String.valueOf(256), val);
            assert false : " should have thrown an exception";
        }
        catch(ArrayIndexOutOfBoundsException ex) {
            System.out.println("got exception as expected: " + ex);
        }
    }

    public void testCopy() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A").setFlag((short)16).setFlag((short)32);
        uuid.put("name", Util.objectToByteBuffer("Bela"))
          .put("age",Util.objectToByteBuffer(49))
          .put("bool",Util.objectToByteBuffer(true));

        ExtendedUUID uuid2=new ExtendedUUID(uuid);
        assert uuid.getMostSignificantBits() == uuid2.getMostSignificantBits();
        assert uuid.getLeastSignificantBits() == uuid2.getLeastSignificantBits();
        assert uuid2.isFlagSet((short)16) && uuid2.isFlagSet((short)32);
        assert uuid2.length() == 3;
        for(String key: Arrays.asList("name", "age", "bool"))
            assert uuid2.keyExists(key);
    }

    public void testMarshalling() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A").setFlag((short)16).setFlag((short)32);
        uuid.put("name", Util.objectToByteBuffer("Bela"))
          .put("age",Util.objectToByteBuffer(49))
          .put("bool",Util.objectToByteBuffer(true));
        int size=uuid.serializedSize();
        byte[] buffer=Util.streamableToByteBuffer(uuid);
        assert size == buffer.length : "expected size of " + size + ", but got " + buffer.length;
        ExtendedUUID uuid2=Util.streamableFromByteBuffer(ExtendedUUID::new, buffer);
        assert uuid2.isFlagSet((short)16);
        assert uuid2.isFlagSet((short)32);
        for(String key: Arrays.asList("name", "age", "bool"))
            assert uuid2.keyExists(key);
    }


    public void testMarshallingNullHashMap() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A");
        int size=uuid.serializedSize();
        byte[] buffer=Util.streamableToByteBuffer(uuid);
        assert size == buffer.length : "expected size of " + size + ", but got " + buffer.length;
        Util.streamableFromByteBuffer(ExtendedUUID::new, buffer);
    }

    public void testMarshallingLargeValues() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A");
        for(int i=1; i <= 5; i++)
            uuid.put(String.valueOf(i), new byte[250]);
        System.out.println("uuid = " + uuid);
        int size=uuid.serializedSize();
        byte[] buffer=Util.streamableToByteBuffer(uuid);
        assert size == buffer.length : "expected size of " + size + ", but got " + buffer.length;
        ExtendedUUID uuid2=Util.streamableFromByteBuffer(ExtendedUUID::new, buffer);
        System.out.println("uuid2 = " + uuid2);
        for(int i=1; i <= 5; i++) {
            byte[] val=uuid.get(String.valueOf(i));
            assert val != null && val.length == 250;
        }
    }

    public void testmarshallingWithNullValues() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A");
        byte[] value=Util.objectToByteBuffer("Bela");
        for(int i=1; i <= 5; i++)
            uuid.put(String.valueOf(i), i % 2 == 0? value : null);
        int size=uuid.serializedSize();
        byte[] buffer=Util.streamableToByteBuffer(uuid);
        assert size == buffer.length : "expected size of " + size + ", but got " + buffer.length;
        ExtendedUUID uuid2=Util.streamableFromByteBuffer(ExtendedUUID::new, buffer);
        for(int i=1; i <= 5; i++) {
            byte[] val=uuid.get(String.valueOf(i));
            boolean null_val=i % 2 != 0;
            if(null_val)
                assert val == null;
            else
                assert val != null && val.length == value.length;
        }
    }

    public void testMarshallingWithRemoval() throws Exception {
        ExtendedUUID uuid=ExtendedUUID.randomUUID("A");
        byte[] value=Util.objectToByteBuffer("Bela");

        for(int i=1; i <= 10; i++)
            uuid.put(String.valueOf(i), value);
        assert uuid.length() == 10;

        for(int i=1; i <= 10; i++)
            if(i % 2 == 0)
                uuid.remove(String.valueOf(i));
        assert uuid.length() == 5;

        int size=uuid.serializedSize();
        byte[] buffer=Util.streamableToByteBuffer(uuid);
        assert size == buffer.length : "expected size of " + size + ", but got " + buffer.length;
        ExtendedUUID uuid2=Util.streamableFromByteBuffer(ExtendedUUID::new, buffer);
        assert uuid2.length() == 5;

        for(int i=1; i <= 10; i++) {
            boolean exists=i % 2 != 0;
            assert uuid2.keyExists(String.valueOf(i)) == exists;
        }
    }



    public void testSiteMaster() {
        SiteMaster sm1=new SiteMaster("sfo"), sm2=new SiteMaster("nyc"), sm3=new SiteMaster("sfo");
        assert !sm1.equals(sm2);
        assert sm1.equals(sm3);
        assert sm1.compareTo(sm3) == 0;

        long hc1=sm1.hashCode(), hc2=sm2.hashCode(), hc3=sm3.hashCode();
        assert hc1 == hc3;
        assert hc1 != hc2;

        Map<SiteMaster,Integer> map=new HashMap<>(3);
        map.put(sm1, 1); map.put(sm2, 2); map.put(sm3, 3);
        System.out.println("map = " + map);
        assert map.size() == 2;
        assert map.get(sm3) == 3;
        assert map.get(sm1) == 3;
        assert map.get(sm2) == 2;
    }

    public void testSiteUUID() throws Exception {
        UUID a=(UUID)Util.createRandomAddress("A"), b=(UUID)Util.createRandomAddress("B");
        SiteUUID sa=new SiteUUID(a, "sm-a", "sfo");
        SiteUUID sb=new SiteUUID(b, "b", "lon")
          .put(Util.stringToBytes("id"),Util.objectToByteBuffer(322649)).setFlag(RELAY2.can_become_site_master_flag);
        System.out.println("sb = " + sb);
        assert sa.getName().equals("sm-a");
        assert sa.getSite().equals("sfo");
        SiteUUID copy=(SiteUUID)sb.copy();
        assert copy.equals(sb);
        assert copy.getName().equals(sb.getName());
        assert copy.getSite().equals(sb.getSite());
    }
}
