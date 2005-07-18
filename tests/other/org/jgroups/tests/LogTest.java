package org.jgroups.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * @author Bela Ban
 * @version $Id: LogTest.java,v 1.1 2005/07/18 14:27:18 belaban Exp $
 */
public class LogTest {

    void start() {
        Log log=LogFactory.getLog(LogTest.class);

        if(log.isTraceEnabled()) {
            log.trace("trace");
        }

        if(log.isDebugEnabled()) {
            log.debug("debug");
        }

        if(log.isInfoEnabled()) {
            log.info("info");
        }

        if(log.isWarnEnabled()) {
            log.warn("warn");
        }

        if(log.isErrorEnabled()) {
            log.error("error");
        }

        if(log.isFatalEnabled()) {
            log.fatal("fatal");
        }

    }

    public static void main(String[] args) {
        new LogTest().start();
    }

}
