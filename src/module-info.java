module org.jgroups {
    requires java.xml;
    requires java.naming;
    requires java.logging;
    requires java.sql;
    requires java.management;
    requires java.security.jgss;
    requires java.scripting;

    // Only used in Demos
    requires static java.desktop;

    // Logging defaults to java.logging
    requires static org.slf4j;
    requires static org.apache.logging.log4j;
    requires static org.apache.logging.log4j.core;

    exports org.jgroups;
    exports org.jgroups.annotations;
    exports org.jgroups.auth;
    exports org.jgroups.blocks;
    exports org.jgroups.blocks.atomic;
    exports org.jgroups.blocks.cs;
    exports org.jgroups.client;
    exports org.jgroups.conf;
    exports org.jgroups.demos;
    exports org.jgroups.fork;
    exports org.jgroups.jmx;
    exports org.jgroups.logging;
    exports org.jgroups.nio;
    exports org.jgroups.protocols;
    exports org.jgroups.protocols.dns;
    exports org.jgroups.protocols.pbcast;
    exports org.jgroups.protocols.relay;
    exports org.jgroups.protocols.relay.config;
}