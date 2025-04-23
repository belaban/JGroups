package org.jgroups.blocks.cs;

import java.io.DataInput;
import org.jgroups.PhysicalAddress;

/**
 * An impl of {@link Receiver}. Will get removed with the switch to Java 8; instead we'll use a default impl in Receiver
 * @author Bela Ban
 * @since  3.6.5
 */
public class ReceiverAdapter implements Receiver {

    @Override
    public void receive(PhysicalAddress sender, byte[] buf, int offset, int length) {

    }

    @Override
    public void receive(PhysicalAddress sender, DataInput in, int length) throws Exception {
        
    }
}
