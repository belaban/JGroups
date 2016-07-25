package org.jgroups.protocols.rules;

import org.jgroups.Event;
import org.jgroups.Message;

/**
 * Interface which defines 2 callbacks: up() and down(). An EventHandler can be installed in SUPERVISOR by a rule which
 * requires callbacks when an event is passed up or down the stack.
 * @author Bela Ban
 * @since  3.3
 */
public interface EventHandler {
    /**
     * Called when an up event is received
     * @param evt The event
     * @return Ignored
     */
    Object up(Event evt) throws Throwable;
    Object up(Message msg) throws Throwable;

    /**
     * Called when a down event is received
     * @param evt The event
     * @return Ignored
     */
    Object down(Event evt) throws Throwable;

    Object down(Message msg) throws Throwable;
}
