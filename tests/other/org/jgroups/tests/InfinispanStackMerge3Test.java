package org.jgroups.tests;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.FRAG3;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.MFC;
import org.jgroups.protocols.MPING;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.VERIFY_SUSPECT;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;

/**
 * This test runs number of merge events with a similar stack configuration that current Infinispan and WildFly is using to
 * showcase speed of merges after partitioning.
 *
 * @author Radoslav Husar
 */
public class InfinispanStackMerge3Test {

    private static List<Supplier<TP>> TRANSPORTS;
    private static int RUNS;
    private static int MEMBERS;

    static {
        TRANSPORTS = new LinkedList<>();
        TRANSPORTS.add(TCP::new);
        TRANSPORTS.add(UDP::new);

        RUNS = Integer.valueOf(System.getProperty("runs", "10"));

        MEMBERS = Integer.valueOf(System.getProperty("members", "10"));
    }

    public static void main(String[] args) throws Exception {
        Map<String, List<Long>> results = new HashMap<>();

        for (Supplier<TP> transport : TRANSPORTS) {
            results.put(transport.get().getClass().getSimpleName(), new LinkedList<>());

            for (int run = 0; run < RUNS; run++) {
                JChannel[] channels = new JChannel[MEMBERS];

                // Setup
                for (int member = 0; member < MEMBERS; member++) {
                    channels[member] = createChannel(transport.get(),
                            InfinispanStackMerge3Test.class.getSimpleName() + member);
                }
                Util.waitUntilAllChannelsHaveSameView(10_000, 100, channels);

                // Partition
                Stream.of(channels).forEach(channel -> {
                    GMS gms = channel.getProtocolStack().findProtocol(GMS.class);
                    View view = View.create(channel.getAddress(), gms.getViewId().getId() + 1, channel.getAddress());
                    gms.installView(view);
                });

                // Merge
                long timeBeforeMerge = System.currentTimeMillis();
                Util.waitUntilAllChannelsHaveSameView(360_000, 100, channels);
                long timeAfterMerge = System.currentTimeMillis();

                // Cleanup
                Stream.of(channels).forEach(JChannel::close);

                results.get(transport.get().getClass().getSimpleName()).add(timeAfterMerge - timeBeforeMerge);
            }
        }

        printStats(results);
    }

    protected static JChannel createChannel(TP transport, String name) throws Exception {
        // Workaround FD_ALL not being builder-like
        FD_ALL fd_all = new FD_ALL();
        fd_all.setInterval(15_000);
        fd_all.setTimeout(60_000);
        fd_all.setTimeoutCheckInterval(5_000);

        JChannel channel = new JChannel(
                transport,
                new MPING(),
                new MERGE3()
                        .setMinInterval(10_000)
                        .setMaxInterval(30_000),
                new FD_SOCK(),
                fd_all,
                new VERIFY_SUSPECT()
                        .setTimeout(5_000),
                new NAKACK2()
                        .setUseMcastXmit(false),
                new UNICAST3(),
                new STABLE(),
                new GMS(),
                new MFC(),
                new FRAG3()
                        .fragSize(8_000)
        );
        channel.setName(name);
        channel.connect(InfinispanStackMerge3Test.class.getSimpleName());
        return channel;
    }

    private static void printStats(Map<String, List<Long>> results) {
        for (Map.Entry<String, List<Long>> entry : results.entrySet()) {
            System.out.printf("Results for %s%n", entry.getKey());
            System.out.printf("Min merge time=%d%n", entry.getValue().stream().min(Long::compare).get());
            System.out.printf("Max merge time=%d%n", entry.getValue().stream().max(Long::compare).get());
            System.out.printf("Avg merge time=%s%n", entry.getValue().stream().mapToLong(x -> x).average().getAsDouble());
            System.out.printf("===================================================================%n");
        }
    }
}
