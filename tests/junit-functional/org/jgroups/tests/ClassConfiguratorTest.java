package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  4.0.8
 */
@Test(groups=Global.FUNCTIONAL)
public class ClassConfiguratorTest {
    protected static final short ID=5678;

    public void testAddProtocol() {
        ClassConfigurator.addProtocol(ID, MyProtocol.class);

        short id=ClassConfigurator.getProtocolId(MyProtocol.class);
        assert id ==  ID;

        Class<?> prot_class=ClassConfigurator.getProtocol(ID);
        assert prot_class == MyProtocol.class;
    }




    protected static class MyProtocol extends Protocol {
        public short getId() {return ID;}
    }
}
