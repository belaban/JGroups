package org.jgroups.admin;

import org.jgroups.JChannel;
import org.jgroups.Message;

/**
 * Handles requests / responses in {@link Admin}
 * @author Bela Ban
 * @since  5.6.0
 */
public abstract class AdminHandler {
    protected final JChannel     ch;

    protected AdminHandler(JChannel ch) {
        this.ch=ch;
    }

    void handleMessage(JChannel ch, Message msg, AdminHeader hdr) {}

    public void sendRequest(String key, Message msg) throws Exception {
        msg.putHeader(Admin.ID, new AdminHeader(key, AdminHeader.Type.request));
        ch.send(msg);
    }

    public void sendResponse(String key, Message msg) throws Exception {
        msg.putHeader(Admin.ID, new AdminHeader(key, AdminHeader.Type.response));
        ch.send(msg);
    }
}
