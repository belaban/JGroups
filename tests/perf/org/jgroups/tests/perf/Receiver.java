package org.jgroups.tests.perf;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 */
public interface Receiver {
    void receive(Object sender, byte[] payload);
}
