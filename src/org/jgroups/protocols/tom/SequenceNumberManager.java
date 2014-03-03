package org.jgroups.protocols.tom;

/**
 * Manages the messages sequence number (keeps it up-to-date)
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class SequenceNumberManager {

    private long sequenceNumber = 0;

    /**
     * 
     * @return the next sequence number
     */
    public long getAndIncrement() {
        return sequenceNumber++;
    }

    /**
     * updates the sequence number to the maximum between them
     * @param otherSequenceNumber   the sequence number received
     */
    public void update(long otherSequenceNumber) {
        sequenceNumber = Math.max(sequenceNumber, otherSequenceNumber + 1);
    }

    /**
     * updates the sequence number and returns the next, that will be used a propose sequence number 
     * @param otherSequenceNumber   the sequence number received
     * @return                      the next sequence number or the received sequence number, if the received sequence
     *                              number is higher the the actual sequence number
     */
    public long updateAndGet(long otherSequenceNumber) {
        if (sequenceNumber >= otherSequenceNumber) {
            return sequenceNumber++;
        } else {
            sequenceNumber = otherSequenceNumber + 1;
            return otherSequenceNumber;
        }
    }

    public long get() {
        return sequenceNumber;
    }
}
