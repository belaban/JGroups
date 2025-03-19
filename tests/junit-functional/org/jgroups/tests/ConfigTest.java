package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.TCP;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Map;

/**
 * Tests correct parsing of attribute values: https://issues.redhat.com/browse/JGRP-2867
 * @author Bela Ban
 * @since  5.4.5, 5.3.16
 */
@Test(groups=Global.FUNCTIONAL)
public class ConfigTest {

    public void test1() throws IOException {
        File f=generate(Map.of("sock_conn_timeout", "500ms",
                               "max_send_queue", "1024",
                               "recv_buf_size", "2m",
                               "peer_addr_read_timeout", "0.5m"));
        try(InputStream in=new FileInputStream(f);JChannel ch=new JChannel(in)) {
            TCP tcp=(TCP)ch.stack().getTransport();
            int timeout=tcp.getSockConnTimeout();
            assert timeout == 500;
            int max_send_queue=tcp.maxSendQueue();
            assert max_send_queue == 1024;
            int recv_buf_size=tcp.getRecvBufSize();
            assert recv_buf_size == 2_000_000;
            int peer_addr_read_timeout=tcp.getPeerAddrReadTimeout();
            assert peer_addr_read_timeout == 30_000;
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            f.delete();
        }

    }

    protected static File generate(Map<String,String> attrs) throws IOException {
        File f=File.createTempFile("config-test", "xml");
        StringBuilder sb=new StringBuilder("<config>\n  <TCP");
        if(attrs != null) {
            for(Map.Entry<String,String> e: attrs.entrySet()) {
                sb.append(String.format(" %s=\"%s\"", e.getKey(), e.getValue()));
            }
        }

        sb.append(" />\n</config>");
        OutputStream os=new FileOutputStream(f);
        os.write(sb.toString().getBytes());
        os.close();
        return f;
    }

}
