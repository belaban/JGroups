package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests partition and merge of {@link FILE_PING} (https://issues.jboss.org/browse/JGRP-2288)
 * @author Bela Ban
 * @since  4.0.17
 */
@Test(groups=Global.FUNCTIONAL)
public class FILE_PING_Test {
    protected File                location;
    protected JChannel            a,b,c,d;
    protected static final String GROUP=FILE_PING_Test.class.getSimpleName();

    @BeforeTest protected void createTempDirectory() throws IOException {
        location=new File(System.getProperty("java.io.tmpdir"), File.separator + FILE_PING_Test.class.getSimpleName());
        if(!location.exists())
            location.mkdir();
    }

    @AfterTest protected void removeTempDirectory() {
        location.delete();
    }

    @BeforeMethod protected void setup() throws Exception {
        a=create("A", location.toString(), 1).connect(GROUP);
        b=create("B", location.toString(), 3).connect(GROUP); // not merge leader
        c=create("C", location.toString(), 2).connect(GROUP); // merge leader (lowest UUID)
        d=create("D", location.toString(), 4).connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b,c,d);
        assert a.getView().getCoord().equals(a.getAddress()); // A is the coord
    }

    @AfterMethod protected void destroy() {Util.close(d,c,b,a);}

    public void testPartitionAndMerge() throws Exception {
        Stream.of(a, b).forEach(ch -> {
            DISCARD disc=ch.getProtocolStack().findProtocol(DISCARD.class);
            disc.addIgnoreMember(c.getAddress()).addIgnoreMember(d.getAddress());
        });
        Stream.of(c, d).forEach(ch -> {
            DISCARD disc=ch.getProtocolStack().findProtocol(DISCARD.class);
            disc.addIgnoreMember(a.getAddress()).addIgnoreMember(b.getAddress());
        });

        System.out.println("** Injecting partition");
        injectView(a,b); // installs view {A,B} in A and B
        injectView(c,d); // installs view {C,D} in C and D

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, c,d);

        System.out.printf("views:\n%s\n",
                          Stream.of(a,b,c,d).map(ch -> ch.getAddress() + ": " + ch.getView()).collect(Collectors.joining("\n")));

        // remove all files
        ((FILE_PING)b.getProtocolStack().findProtocol(FILE_PING.class)).removeAll(GROUP);

        System.out.println("A leaves the cluster:");
        a.close();
        Util.waitUntilAllChannelsHaveSameView(10_000, 1000, b);
        System.out.printf("new views:\n%s\n",
                          Stream.of(b,c,d).map(ch -> ch.getAddress() + ": " + ch.getView()).collect(Collectors.joining("\n")));

        Stream.of(a,b,c,d).forEach(ch -> ch.getProtocolStack().removeProtocol(DISCARD.class));

        System.out.println("waiting for partitions to merge");
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, b,c,d);
        System.out.printf("merged views:\n%s\n",
                          Stream.of(b,c,d).map(ch -> ch.getAddress() + ": " + ch.getView()).collect(Collectors.joining("\n")));

    }

    protected static void injectView(JChannel ... channels) {
        Address coord=channels[0].getAddress();
        long current_view_id=((GMS)channels[0].getProtocolStack().findProtocol(GMS.class)).view().getViewId().getId();
        View v=View.create(coord, current_view_id+1, Stream.of(channels).map(JChannel::getAddress)
          .collect(Collectors.toList()));
        Stream.of(channels).forEach(ch -> ((GMS)ch.getProtocolStack().findProtocol(GMS.class)).installView(v));
    }



    protected static JChannel create(String name, String location, long uuid) throws Exception {
        return new JChannel(new TCP().setBindAddress(Util.getLoopback()).setValue("bundler_type", "nb"),
                            new DISCARD(),
                            new FILE_PING().setRemoveAllDataOnViewChange(true).setLocation(location)
                              .setValue("write_data_on_find", true)
                              .setValue("info_writer_max_writes_after_view", 0)
                              .setValue("info_writer_sleep_time", 2000),
                            new MERGE3().setMinInterval(2000).setMaxInterval(5000),
                            new FD_SOCK(), new NAKACK2(), new UNICAST3(), new STABLE(), new GMS().joinTimeout(3000))
          .addAddressGenerator(() -> new UUID(uuid, 0))
          .name(name);
    }
}
