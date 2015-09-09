package org.jgroups.blocks.cs;

/**
 * @author Bela Ban
 * @since  3.6.5
 */
public interface ConnectionListener {
    void connectionClosed(Connection conn, String reason);
    void connectionEstablished(Connection conn);
}
