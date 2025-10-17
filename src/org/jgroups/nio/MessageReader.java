package org.jgroups.nio;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.jgroups.util.Util;

/**
 * Class for reading length prefix messages from a channel.
 * We have an internal buffer which is compacted and grown as needed.
 * We have a separate reader index, and buffer.position() is used as the writer index; this is similar to Netty ByteBuf.
 * The limit is not used and always same as capacity.
 * 
 * @author Christian Fredriksson
 * @since 5.5.1
 */
public class MessageReader {

    private final SocketChannel channel;
    private ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
    private int readerIndex;
    protected int max_length; // max number of bytes to read (JGRP-2523)

    public MessageReader(SocketChannel channel) {
        this.channel = channel;
    }

    /**
     * Reads length prefixed messages from a channel.
     * 
     * @param ch The channel to read data from
     * @return The buffer (position is 0 and limit is length), or null if not all data could be read.
     */
    public ByteBuffer readMessage() throws IOException {

        ByteBuffer message;

        if (readerIndex + 4 <= buffer.position()) {
            // Check if we have a pending message already
            int length = getLength();
            if ((message = tryGetMessage(length)) != null) {
                return message;
            }
        }

        // Make at least enough room for length
        if (readerIndex + 4 > buffer.capacity()) {
            makeSpace(4);
        }

        // Fill as much data as possible
        if (!fillBuffer()) {
            return null;
        }

        if (buffer.position() - readerIndex < 4) {
            // Did not even get length
            return null;
        }

        // Check if we have a full message now
        int length = getLength();
        if ((message = tryGetMessage(length)) != null) {
            return message;
        }

        // Buffer got full, expand and try again
        if (!buffer.hasRemaining()) {
            makeSpace(4 + length);
            if (!fillBuffer()) {
                return null;
            }
            if ((message = tryGetMessage(length)) != null) {
                return message;
            }
        }

        return null;
    }

    private boolean fillBuffer() throws IOException {
        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
            throw new EOFException();
        }
        return bytesRead > 0;
    }

    private int getLength() throws IOException {
        int length = buffer.getInt(readerIndex);
        // Check max_length constraint
        if (max_length > 0 && length > max_length)
            throw new IllegalStateException(String.format("the length of a message (%s) from %s is bigger than the " +
                    "max accepted length (%s): discarding the message",
                    Util.printBytes(length), channel.getRemoteAddress(),
                    Util.printBytes(max_length)));
        return length;
    }

    /**
     * Position should be at start of the message (before the length)
     */
    private ByteBuffer tryGetMessage(int length) {
        if (readerIndex + 4 + length <= buffer.position()) {
            ByteBuffer message = buffer.slice(readerIndex + 4, length);
            readerIndex = readerIndex + 4 + length;
            if (readerIndex == buffer.position()) {
                buffer.position(0);
                readerIndex = 0;
            }
            return message;
        }
        return null;
    }

    /**
     * Ensures the buffer will fit at least the total space either by compacting or allocating a bigger buffer.
     */
    private void makeSpace(int totalSpace) {
        buffer.limit(buffer.position());
        buffer.position(readerIndex);
        readerIndex = 0;
        if (totalSpace <= buffer.capacity()) {
            buffer.compact();
        } else {
            int newCapacity = Math.max(totalSpace, buffer.capacity() * 2);
            ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCapacity);
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
    }

    public MessageReader maxLength(int max_length) {
        this.max_length = max_length;
        return this;
    }

    @Override
    public String toString() {
        return String.format("[readerIndex=%d writerIndex=%d capacity=%d remaining=%d]", readerIndex, buffer.position(), buffer.capacity(),
                buffer.position() - readerIndex);
    }
}
