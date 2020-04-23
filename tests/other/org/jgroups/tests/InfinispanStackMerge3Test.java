package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This test runs number of merge events with a similar stack configuration that current Infinispan and WildFly is using to
 * showcase speed of merges after partitioning.
 *
 * @author Radoslav Husar
 */
public class InfinispanStackMerge3Test {

    private static final List<Supplier<TP>> TRANSPORTS;
    private static final int                RUNS;
    private static final int                MEMBERS;

    static {
        TRANSPORTS =Arrays.asList(TCP::new,UDP::new);

        RUNS = Integer.valueOf(System.getProperty("runs", "5"));
        MEMBERS = Integer.valueOf(System.getProperty("members", "10"));
    }

    public static void main(String[] args) throws Exception {
        Map<String, List<Long>> results = new HashMap<>();

        for (Supplier<TP> transport : TRANSPORTS) {
            results.put(transport.get().getClass().getSimpleName(), new LinkedList<>());

            for (int run = 0; run < RUNS; run++) {
                JChannel[] channels = new JChannel[MEMBERS];

                // Setup
                for (int member = 0; member < MEMBERS; member++)
                    channels[member] = createChannel(transport.get(), String.valueOf(member+1));
                Util.waitUntilAllChannelsHaveSameView(10_000, 500, channels);
                System.out.printf("-- run #%d: cluster formed:\n%s\n", run+1, printViews(channels));

                // Partition
                Stream.of(channels).forEach(channel -> {
                    GMS gms = channel.getProtocolStack().findProtocol(GMS.class);
                    View view = View.create(channel.getAddress(), gms.getViewId().getId() + 1, channel.getAddress());
                    gms.installView(view);
                });

                System.out.printf("-- injected partition (waiting for merge):\n%s\n", printViews(channels));

                // Merge
                long timeBeforeMerge = System.currentTimeMillis();
                Util.waitUntilAllChannelsHaveSameView(360_000, 100, channels);
                long timeAfterMerge = System.currentTimeMillis();

                System.out.printf("-- run #%d: %s cluster merged in %d ms:\n%s\n",
                                  run+1, transport.get().getClass().getSimpleName(), timeAfterMerge - timeBeforeMerge, printViews(channels));

                // Cleanup
                Stream.of(channels).forEach(JChannel::close);

                results.get(transport.get().getClass().getSimpleName()).add(timeAfterMerge - timeBeforeMerge);
            }
        }

        printStats(results);
    }

    protected static String printViews(JChannel ... channels) {
        return Stream.of(channels).map(ch -> String.format("%s: %s", ch.getAddress(), ch.getView()))
          .collect(Collectors.joining("\n"));

    }

    protected static JChannel createChannel(TP transport, String name) throws Exception {
        return new JChannel(transport.setBindAddress(Util.getLoopback()),
                            new MPING(),
                            new MERGE3().setMinInterval(5000).setMaxInterval(10000),
                            new FD_SOCK(),
                            new FD_ALL3().setTimeout(8000).setInterval(2000),
                            new VERIFY_SUSPECT().setTimeout(5000),
                            new NAKACK2().useMcastXmit(false),
                            new UNICAST3(),
                            new STABLE(),
                            new GMS().setJoinTimeout(1000).printLocalAddress(false),
                            new MFC(),
                            new FRAG3().setFragSize(8000)
        ).setName(name).connect(InfinispanStackMerge3Test.class.getSimpleName());
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
