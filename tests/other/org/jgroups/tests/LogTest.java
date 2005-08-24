// $Id: LogTest.java,v 1.3 2005/08/24 04:47:11 belaban Exp $

package org.jgroups.tests;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Computes cost of logging
 */
public class LogTest extends TestCase {
    static final Log log=LogFactory.getLog(LogTest.class);
    final boolean trace=log.isTraceEnabled();
    final int NUM=10000;
    long start, stop, diff;

    public LogTest(String name) {
        super(name);
    }


    public void testSpeedWithSingleTraceStatement() {
        start=System.currentTimeMillis();
        for(int i=0; i < NUM; i++) {
            if(log.isTraceEnabled()) {
                log.trace("this is log statement number " + i + " from Bela");
            }
        }
        stop=System.currentTimeMillis();
        System.out.println("took " + (stop-start) + "ms for " + NUM + " log statements");
    }

    public void testSpeedWithSingleTraceStatementLogIsTracePreset() {
        start=System.currentTimeMillis();
        for(int i=0; i < NUM; i++) {
            if(trace) {
                log.trace("this is log statement number " + i + " from Bela");
            }
        }
        stop=System.currentTimeMillis();
        System.out.println("took " + (stop-start) + "ms for " + NUM + " log statements");
    }

    public void testSpeedWithTwoTraceStatements() {
        start=System.currentTimeMillis();
        for(int i=0; i < NUM; i++) {
            if(log.isTraceEnabled()) {
                log.trace("this is log statement number " + i);
                log.trace(" from Bela");
            }
        }
        stop=System.currentTimeMillis();
        System.out.println("took " + (stop-start) + "ms for " + NUM + " log statements");
    }


    public static void main(String[] args) {
        String[] testCaseName={LogTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
