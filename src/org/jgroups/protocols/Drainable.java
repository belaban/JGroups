package org.jgroups.protocols;

import java.util.concurrent.TimeoutException;

/**
 * Implemented by protocols such as {@link UNICAST4} or {@link org.jgroups.protocols.pbcast.NAKACK2} to support
 * message draining (docs/design/MessageDraining.txt). JIRA: https://redhat.atlassian.net/browse/JGRP-3049
 * @author Bela Ban
 * @since  5.6.0
 */
public interface Drainable {
    default void drain(long timeout) throws TimeoutException {
        drain(timeout, timeout/10, true, true);
    }


    default void drain(long timeout, long interval, boolean send_table, boolean recv_table) throws TimeoutException {
        throw new UnsupportedOperationException(String.format("%s: %s is not implemented",
                                                              getClass().getSimpleName(), Drainable.class.getSimpleName()));
    }
}
