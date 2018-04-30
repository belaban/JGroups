package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.FILE_PING;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Tests original coordinator not being marked as coord, and firstOfAllClients() failing
 * (https://issues.jboss.org/browse/JGRP-2262)
 * @author Bela Ban
 * @since  4.0.12
 */
@Test(groups=Global.FUNCTIONAL,invocationTimeOut=10000)
public class FrozenCoordinatorTest {
    protected String   location;
    protected Path     root_dir;
    protected File     file;
    protected JChannel b, c;

    protected static final String CLUSTER="FrozenCoordinator";
    protected static final String FILENAME="frozen_coord.A.list";
    protected static final String LOWEST_UUID=new UUID(Long.MIN_VALUE, Long.MIN_VALUE).toStringLong();
    protected static final String INFO=String.format("A \t%s \t127.0.0.1:12345 \tF\n", LOWEST_UUID);


    @BeforeClass
    protected void setup() throws Exception {
        root_dir=Files.createTempDirectory(null);
        root_dir.toFile().deleteOnExit();
        file=new File(root_dir.toFile(), CLUSTER);
        file.mkdirs();
        file.deleteOnExit();
        file=new File(file, FILENAME);
        file.createNewFile();
        if(!file.exists())
            throw new IllegalStateException(String.format("unable to create file %s", file.toString()));
        file.deleteOnExit();
        location=file.toString();
    }

    @AfterClass protected void destroy() {
        Util.close(b);
    }


    /**
     * Writes the entry of the coordinator to the file, but sets coord to 'F' (false). Also, assigns the lowest possible
     * UUID to the coord (A), so that it will always be at the head of the sorted set. Then B is started, who will
     * not find a coord, but still receive A's info, but never become coord as A's UUID is lower. This leads to a
     * never-ending loop, only terminated by @invocationTimeOut. See JGRP-2262 for details.
     */
    public void testFrozenCoord() throws Exception {
        try(OutputStream out=new FileOutputStream(file)) {
            out.write(INFO.getBytes());
        }

        b=create("B");
        b.connect(CLUSTER);
        View view=b.getView();
        System.out.println("view = " + view);
        assert view.size() == 1;
        assert view.containsMember(b.getAddress());

        c=create("C");
        c.connect(CLUSTER);

        System.out.printf("B's view: %s\nC's view: %s\n", b.getView(), c.getView());
        for(View v: Arrays.asList(b.getView(), c.getView())) {
            assert v.size() == 2;
            assert v.containsMember(b.getAddress()) && v.containsMember(c.getAddress());
        }
    }


    protected JChannel create(String name) throws Exception {
        FILE_PING ping=new FILE_PING().setLocation(root_dir.toString());
        ping.setValue("remove_all_files_on_view_change", true);
        ping.setValue("info_writer_max_writes_after_view", 10);
        ping.setValue("info_writer_sleep_time", 500);

        GMS gms=new GMS();
        gms.joinTimeout(1000);
        gms.setMaxJoinAttempts(5);

        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          ping,
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          gms
        };
        return new JChannel(protocols).name(name);
    }
}
