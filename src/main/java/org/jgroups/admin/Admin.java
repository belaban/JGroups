package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Util;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Class to manage JGroups-internal cluster wide requests and responses (https://redhat.atlassian.net/browse/JGRP-3043).
 * An instance is always associated with a {@link JChannel}
 * @author Bela Ban
 * @since  5.6.0
 */
public class Admin implements Lifecycle {
    protected final JChannel                  ch;
    protected final Log                       log;
    protected long                            timeout=1000;
    public static final short                 ID;
    public static final String                ADMIN_KEY="admin";
    public static final String                ADMIN_EXEC_KEY="admin-exec";
    public static final String                INSTALL_HANDLER="install-handler"; // | <key> | <classname> |
    public static final String                REMOVE_HANDLER="remove-handler"; // | <key> |
    public static final String                GET_HANDLERS="get-handlers";
    protected final Map<String,AdminHandler>  handlers=new ConcurrentHashMap<>();
    protected final ResponseCollector<String> rsps=new ResponseCollector<>();

    static {
        ID=ClassConfigurator.getProtocolId(Admin.class);
    }

    public Admin(JChannel ch) {
        this.ch=ch;
        this.log=ch.log();
    }

    public JChannel  channel()             {return ch;}
    public long      timeout()             {return timeout;}
    public Admin     timeout(long timeout) {this.timeout=timeout; return this;}

    public <T extends AdminHandler> T get(String key) {return (T)handlers.get(key);}

    public Admin register(String key, AdminHandler handler) {
        if(ADMIN_KEY.equals(key) || ADMIN_EXEC_KEY.equals(key))
            throw new IllegalStateException(String.format("key %s is reserved for internal use", key));
        if(handlers.containsKey(key))
            throw new IllegalStateException(String.format("key %s already exists", key));
        handlers.put(key, handler);
        return this;
    }

    public Admin deregister(String key) {
        if(key != null)
            handlers.remove(key);
        return this;
    }

    public void handleAdminMessage(Message msg, AdminHeader hdr) {
        String key=hdr.key();
        switch(key) {
            case ADMIN_KEY:
                handleInternalAdminMessage(msg, hdr);
                break;
            case ADMIN_EXEC_KEY:
                handleExec(msg, hdr);
                break;
            default:
                AdminHandler handler=getHandler(key);
                if(handler != null)
                    handler.handleMessage(msg, hdr);
        }
    }

    public String print() {
        return handlers.entrySet().stream()
          .map(e -> String.format("%s: %s", e.getKey(), e.getValue().getClass().getName()))
          .collect(Collectors.joining("\n"));
    }

    @Override
    public String toString() {
        return String.format("%d handlers: %s", handlers.size(), handlers.keySet());
    }

    protected AdminHandler getHandler(String key) {
        AdminHandler handler=handlers.get(key);
        if(handler == null)
            log.error(String.format("%s: no admin handler registered for key \"%s\"", ch.address(), key));
        return handler;
    }

    protected void handleInternalAdminMessage(Message msg, AdminHeader hdr) {
        switch(hdr.type()) {
            case request -> handleInternalRequest(msg);
            case response -> handleInternalResponse(msg, hdr);
            default -> ch.log().error("%s: internal type %s not known", ch.address(), hdr.type());
        }
    }

    protected void handleExec(Message msg, AdminHeader hdr) {
        String s=msg.getObject();
        s=s.trim();
        String command=Util.parse(s.trim(), 0);
        if(command == null) {
            ch.log().error("%s: command is null", ch.address());
            return;
        }
        String args=s.substring(command.length()).trim();
        if(command.equals(ADMIN_KEY)) {
            String rsp=sendAdminRequest(null, ADMIN_KEY, args);
            if(rsp != null)
                sendAdminResponse(msg.src(), hdr.key(), rsp);
            return;
        }
        AdminHandler h=getHandler(command);
        if(h != null) {
            String rsp=h.executeRequest(args);
            if(rsp != null) {
                try {
                    ch.send(new ObjectMessage(msg.src(), rsp)
                              .setFlag(Message.Flag.OOB, Message.Flag.NO_FC, Message.Flag.NO_RELIABILITY));
                }
                catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    protected void handleInternalRequest(Message msg) {
        String s=msg.getObject();
        int index=s.indexOf(' ');
        String cmd=index == -1? s : s.substring(0, index).trim();
        s=s.substring(cmd.length()).trim();
        switch(cmd) {
            case INSTALL_HANDLER:
                try {
                    String[] cmdline=s.split("\\s+");
                    String key=cmdline[0], handler_class=cmdline[1];
                    Class<?> cl=Util.loadClass(handler_class, this.getClass());
                    AdminHandler h=(AdminHandler)cl.getDeclaredConstructor(JChannel.class).newInstance(channel());
                    register(key, h);
                }
                catch(ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                      InvocationTargetException e) {
                    channel().log().error("%s: failed loading for cmd=%s: %s",
                                          channel().address(), INSTALL_HANDLER, e.getMessage());
                }
                break;
            case REMOVE_HANDLER:
                String key=s.trim();
                deregister(key);
                break;
            case GET_HANDLERS:
                String tmp=handlers.entrySet().stream()
                  .map(e -> String.format("%s: %s", e.getKey(), e.getValue().getClass().getName()))
                  .collect(Collectors.joining("\n"));
                sendAdminResponse(msg.src(), Admin.ADMIN_KEY, tmp);
                break;
            default:
                log.error("%s: command %s not known", ch.address(), cmd);
        }
    }

    protected void handleInternalResponse(Message msg, AdminHeader hdr) {
        rsps.add(msg.src(), msg.getObject());
    }

    protected String sendAdminRequest(Address dest, String key, String args) {
        rsps.reset(dest == null? ch.view().getMembers() : List.of(dest));
        send(dest, key, args, AdminHeader.Type.request);
        rsps.waitForAllResponses(timeout);
        Map<Address, String> results=rsps.getResults();
        return results == null || results.isEmpty()? "n/a" : print(results);
    }

    protected void sendAdminResponse(Address dest, String key, String args) {
        send(dest, key, args, AdminHeader.Type.response);
    }

    protected void send(Address dest, String key, String args, AdminHeader.Type type) {
        Message msg=new ObjectMessage(dest, args).putHeader(Admin.ID, new AdminHeader(key, type));
        try {
            ch.send(msg);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static String print(Map<Address,String> m) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,String> e: m.entrySet()) {
            sb.append(e.getKey()).append(":\n").append(e.getValue()).append("\n\n");
        }
        return sb.toString();
    }

}
