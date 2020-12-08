package org.jgroups.blocks.cs;

import org.jgroups.Address;

import java.io.DataInput;

/**
 * An impl of {@link Receiver}. Will get removed with the switch to Java 8; instead we'll use a default impl in Receiver
 * @author Bela Ban
 * @since  3.6.5
 */
public class ReceiverAdapter implements Receiver {
    public void receive(Address sender, byte[] buf, int offset, int length) {

    }

    public void receive(Address sender, DataInput in) throws Exception {

    }
}
