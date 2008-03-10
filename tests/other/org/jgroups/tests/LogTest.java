// $Id: LogTest.java,v 1.4 2008/03/10 15:39:26 belaban Exp $

package org.jgroups.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;


/**
 * Computes cost of logging
 */
public class LogTest {
    static final Log log=LogFactory.getLog(LogTest.class);
    final boolean trace=log.isTraceEnabled();
    final int NUM=10000;
    long start, stop, diff;

    public LogTest(String name) {
    }


    @Test
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

    @Test
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

    @Test
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
