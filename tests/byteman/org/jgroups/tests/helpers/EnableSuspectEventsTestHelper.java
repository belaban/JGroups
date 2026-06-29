package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Byteman helper for EnableSuspectEventsDeterministicEventTest.
 * <p>
 * Delays the rejoining node's replaceConnection to widen the race window
 * so the other node's concurrent connect completes first, causing rejection
 * and socket close — producing a dead connection in the conns map.
 */
public class EnableSuspectEventsTestHelper extends Helper {

    private static final AtomicBoolean armed = new AtomicBoolean(false);
    private static final AtomicReference<String> armedNodeName = new AtomicReference<>();

    protected EnableSuspectEventsTestHelper(Rule rule) {
        super(rule);
    }

    /** Arm the delay for the given node name (e.g. "A", "B", "C") */
    public static void arm(String nodeName) {
        armedNodeName.set(nodeName);
        armed.set(true);
    }

    public static void disarm() {
        armed.set(false);
        armedNodeName.set(null);
    }

    /**
     * Called by Byteman at replaceConnection entry.
     * Pauses only if armed AND the current thread belongs to the rejoining node.
     * Thread names follow the pattern: pd-bundler-XX,ClusterName,NodeName
     */
    public static void maybePause(Object node, Object dest, Object conn, String threadName) {
        if(!armed.get())
            return;
        String targetNode = armedNodeName.get();
        if(targetNode == null || !threadName.endsWith("," + targetNode))
            return;
        // Fire once
        armed.set(false);

        System.out.println("--> [DELAY] replaceConnection PAUSING node=" + node
                           + " dest=" + dest + " thread=" + threadName);
        try {
            Thread.sleep(3000);
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("--> [DELAY] replaceConnection RESUMING node=" + node);
    }
}
