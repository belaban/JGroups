package org.jgroups.admin;

import org.jgroups.JChannel;
import org.jgroups.Message;

/**
 * Handles requests / responses in {@link Admin}
 * @author Bela Ban
 * @since  5.6.0
 */
public abstract class AdminHandler {
    public static final String OK="OK: ", FAIL="FAIL: ";
    protected final JChannel   ch;

    protected AdminHandler(JChannel ch) {
        this.ch=ch;
    }

    public JChannel channel() {return ch;}

    public void handleMessage(Message msg, AdminHeader hdr) {
        switch(hdr.type()) {
            case request -> handleRequest(msg, hdr);
            case response -> handleResponse(msg, hdr);
        }
    }

    public void sendRequest(String key, Message msg) {
        sendRequest(key, msg, null);
    }

    public void sendRequest(String key, Message msg, String command) {
        try {
            msg.putHeader(Admin.ID, new AdminHeader(key, AdminHeader.Type.request, command));
            ch.send(msg);
        }
        catch(Exception e) {
            ch.log().error("%s: failed sending request: %s", ch.address(), e.getMessage());
        }
    }

    public void sendResponse(String key, Message msg) {
        sendResponse(key, msg, null);
    }

    public void sendResponse(String key, Message msg, String command) {
        try {
            msg.putHeader(Admin.ID, new AdminHeader(key, AdminHeader.Type.response, command));
            ch.send(msg);
        }
        catch(Exception e) {
            ch.log().error("%s: failed sending response: %s", ch.address(), e.getMessage());
        }
    }

    public abstract void handleRequest(Message msg, AdminHeader hdr);

    public abstract void handleResponse(Message msg, AdminHeader hdr);
}
