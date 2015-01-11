package org.jgroups.protocols.jzookeeper;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Class representing message ids
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class MessageID implements SizeStreamable {
    private Address originator = null; // Originator of an ordering request
    private int sequence = -1; // Sequence local to the requesting node

    public MessageID() {
    }

    public MessageID(Address originator, int sequence) {
        this.originator = originator;
        this.sequence = sequence;
    }

    public Address getOriginator() {
        return originator;
    }

    public int getSequence() {
        return sequence;
    }

    @Override
    public int size() {
        return Util.size(originator) + Global.INT_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(originator, out);
        out.writeInt(sequence);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        originator = Util.readAddress(in);
        sequence = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageID messageId = (MessageID) o;

        if (sequence != messageId.sequence) return false;
        if (originator != null ? !originator.equals(messageId.originator) : messageId.originator != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sequence;
        result = 31 * result + (originator != null ? originator.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MessageId{" +
                "sequence=" + sequence +
                ", originator=" + originator +
                '}';
    }
}