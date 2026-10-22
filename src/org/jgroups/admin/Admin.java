package org.jgroups.admin;

import org.jgroups.JChannel;
import org.jgroups.Lifecycle;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;

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
    protected final JChannel                 ch;
    protected final Log                      log;
    public static final short                ID;
    protected final Map<String,AdminHandler> handlers=new ConcurrentHashMap<>();

    static {
        ID=ClassConfigurator.getProtocolId(Admin.class);
    }

    public Admin(JChannel ch) {
        this.ch=ch;
        this.log=ch.log();
    }

    public JChannel                   channel()       {return ch;}
    public <T extends AdminHandler> T get(String key) {return (T)handlers.get(key);}

    public Admin register(String key, AdminHandler handler) {
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
        AdminHandler handler=handlers.get(hdr.key());
        if(handler == null)
            log.error(String.format("%s: no admin handler registered for key \"%s\"", ch.address(), hdr.key()));
        else
            handler.handleMessage(msg, hdr);
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
}
