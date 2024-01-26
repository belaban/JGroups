package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.MessageCache;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;

/**
 * Tests {@link org.jgroups.util.MessageCache}
 * @author Bela Ban
 * @since  5.3.2
 */
@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class MessageCacheTest {
    protected MessageCache cache;
    protected static final Address A=Util.createRandomAddress("A"), B=Util.createRandomAddress("B"),
      C=Util.createRandomAddress("C");

    @BeforeMethod protected void setup() {
        cache=new MessageCache();
    }

    public void testCreation() {
        assert cache.isEmpty();
    }

    public void testAdd() {
        for(int i=1; i <= 5; i++) {
            cache.add(A, new Message(A, i));
            cache.add(B, new Message(B, i+10));
        }
        assert !cache.isEmpty();
        assert cache.size() == 10;
    }

    public void testDrain() {
        testAdd();
        Collection<Message> list=cache.drain(null);
        assert list == null;
        list=cache.drain(C);
        assert list == null;
        list=cache.drain(B);
        assert list.size() == 5;
        assert cache.size() == 5;
        assert !cache.isEmpty();
        list=cache.drain(A);
        assert list.size() == 5;
        assert cache.isEmpty();
    }
}
