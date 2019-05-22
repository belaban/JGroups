package org.jgroups.tests;

import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.NameCache;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Parses messages out of a captured file and writes them to stdout
 * @author Bela Ban
 */
public class ParseMessages {
    protected static final short GMS_ID=ClassConfigurator.getProtocolId(GMS.class);
    protected static boolean show_views=true;

    public static void parse(byte[] buf, int offset, int length, BiConsumer<Short,Message> msg_consumer,
                             BiConsumer<Short,MessageBatch> batch_consumer, boolean tcp) {
        Util.parse(new ByteArrayInputStream(buf, offset, length), msg_consumer, batch_consumer, tcp);
    }

    public static void parse(InputStream in, BiConsumer<Short,Message> msg_consumer,
                             BiConsumer<Short,MessageBatch> batch_consumer, boolean tcp) throws FileNotFoundException {
        Util.parse(in, msg_consumer, batch_consumer, tcp);
    }


    public static void main(String[] args) throws Exception {
        String file=null;
        boolean print_vers=false, binary_to_ascii=true, parse_discovery_responses=true, tcp=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-file")) {
                file=args[++i];
                continue;
            }
            if("-version".equals(args[i])) {
                print_vers=true;
                continue;
            }
            if("-tcp".equalsIgnoreCase(args[i])) {
                tcp=true;
                continue;
            }
            if("-mappings".equalsIgnoreCase(args[i])) {
                readMappings(args[++i]);
                continue;
            }
            if("-binary-to-ascii".equalsIgnoreCase(args[i])) {
                binary_to_ascii=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-parse-discovery-responses".equalsIgnoreCase(args[i])) {
                parse_discovery_responses=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-show-views".equalsIgnoreCase(args[i])) {
                show_views=Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }

        final boolean print_version=print_vers, parse=parse_discovery_responses;
        final AtomicInteger cnt=new AtomicInteger(1);
        BiConsumer<Short,Message> msg_consumer=(version,msg) -> {
            if(parse) {
                try {
                    parseDiscoveryResponse(msg);
                }
                catch(Exception e) {
                    System.err.printf("failed parsing discovery response from %s: %s\n", msg.src(), e);
                }
            }
            View view=show_views? getView(msg) : null;
            System.out.printf("%d:%s %s, hdrs: %s %s\n", cnt.getAndIncrement(),
                              print_version? String.format(" [%s]", Version.print(version)) : "", msg, msg.printHeaders(),
                              view == null? "" : "(view: " + view + ")");
        };

        BiConsumer<Short,MessageBatch> batch_consumer=(version,batch) -> {
            System.out.printf("%d:%s batch to %s from %s (%d messages):\n",
                              cnt.getAndIncrement(),
                              print_version? String.format(" [%s]", Version.print(version)) : "",
                              batch.dest() != null? batch.dest() : "<all>", batch.sender(),
                              batch.size());
            int index=1;
            for(Message msg: batch) {
                if(parse) {
                    try {
                        parseDiscoveryResponse(msg);
                    }
                    catch(Exception e) {
                        System.err.printf("failed parsing discovery response from %s: %s\n", msg.src(), e);
                    }
                }
                View view=show_views? getView(msg) : null;
                System.out.printf("%d:%s %s, hdrs: %s %s\n", cnt.getAndIncrement(),
                                  print_version? String.format(" [%s]", Version.print(version)) : "", msg, msg.printHeaders(),
                                  view == null? "" : "(view: " + view + ")");
                System.out.printf("    %d: [%d bytes%s], hdrs: %s %s\n",
                                  index++, msg.getLength(),
                                  msg.getFlags() > 0? ", flags=" + Message.flagsToString(msg.getFlags()) : "",
                                  msg.printHeaders(),
                                  view == null? "" : "(view: " + view + ")");
            }
        };
        InputStream in=file != null? new FileInputStream(file) : System.in;
        if(binary_to_ascii)
            in=new BinaryToAsciiInputStream(in);
        parse(in, msg_consumer, batch_consumer, tcp);
    }

    protected static void parseDiscoveryResponse(Message msg) throws IOException, ClassNotFoundException {
        Collection<Header> hdrs=msg.getHeaders().values();
        for(Header hdr: hdrs) {
            if(hdr instanceof PingHeader) {
                byte[] payload=msg.getRawBuffer();
                if(payload != null) {
                    PingData data=Util.streamableFromBuffer(PingData::new, payload, msg.getOffset(), msg.getLength());
                    NameCache.add(data.getAddress(), data.getLogicalName());
                }
                break;
            }
        }
    }

    protected static View getView(Message msg) {
        GMS.GmsHeader hdr=msg.getHeader(GMS_ID);
        if(hdr == null)
            return null;
        try {
            switch(hdr.getType()) {
                case GMS.GmsHeader.VIEW:
                    return GMS._readViewAndDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength()).getVal1();
                case GMS.GmsHeader.JOIN_RSP:
                    return Util.streamableFromBuffer(JoinRsp::new, msg.getRawBuffer(), msg.getOffset(), msg.getLength()).getView();
            }
            return null;
        }
        catch(Throwable t) {
            return null;
        }
    }


    protected static void help() {
        System.out.println("ParseMessages [-version] [-file <filename>] [-mappings <filename>] " +
                             "[-tcp] [-binary-to-ascii true|false] [-parse-discovery-responses true|false]\n" +
                             "[-show-views (true|false)\n\n" +
                             "-file: if missing stdin will be used\n" +
                             "-tcp: when TCP is used, the first 4 bytes (length) is skipped\n" +
                             "-mappings: file containing UUIDs and logical name (1 mapping per line)\n" +
                             "-binary-to-ascii <true|false>: if the input contains binary data, convert it to ASCII " +
                             "(required by ParseMessages) on the fly\n" +
                             "-parse-discovery-responses: if true, discovery responses for UUID-logical addr mappings " +
                             "are parsed. This shows logical names rather than UUIDs, making dumps more legible.\n" +
                             "-show-views: shows the views for VIEW and JOIN_RSP messages");
    }

    protected static void readMappings(String filename) throws IOException {
        try(InputStream in=new FileInputStream(filename)) {
            for(;;) {
                String uuid_str=Util.readToken(in);
                String name=Util.readToken(in);

                if(uuid_str == null || name == null)
                    break;

                UUID uuid=null;
                try {
                    long tmp=Long.valueOf(uuid_str);
                    uuid=new UUID(0, tmp);
                }
                catch(Throwable t) {
                    uuid=UUID.fromString(uuid_str);
                }
                NameCache.add(uuid, name);
            }
        }
    }

    protected static class BinaryToAsciiInputStream extends InputStream {
        protected final InputStream in;
        protected final byte[]      input=new byte[2];

        public BinaryToAsciiInputStream(InputStream in) {
            this.in=in;
        }

        @Override public int read() throws IOException {
            input[0]=(byte)in.read();
            if(input[0] == '\n' || input[0] == '\r')
                return read();
            if(input[0] < 0)
                return input[0];
            input[1]=(byte)in.read();
            if(input[1] == '\n')
                return read();
            if(input[1] < 0)
                return input[1];
            String tmp=new String(input);
            int val=Integer.parseInt(tmp, 16);
            return (char)val;
        }

    }
}
