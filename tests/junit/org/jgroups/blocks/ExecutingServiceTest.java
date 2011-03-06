/*
 *  $URL: https://athena.redprairie.com/svn/prod/devtools/trunk/bootstrap/eclipse/codetemplates.xml $
 *  $Author: mlange $
 *  $Date: 2009-06-18 22:49:22 -0500 (Thu, 18 Jun 2009) $
 *  
 *  $Copyright-Start$
 *
 *  Copyright (c) 2011
 *  RedPrairie Corporation
 *  All Rights Reserved
 *
 *  This software is furnished under a corporate license for use on a
 *  single computer system and can be copied (with inclusion of the
 *  above copyright) only for use on such a system.
 *
 *  The information in this document is subject to change without notice
 *  and should not be construed as a commitment by RedPrairie Corporation.
 *
 *  RedPrairie Corporation assumes no responsibility for the use of the
 *  software described in this document on equipment which has not been
 *  supplied or approved by RedPrairie Corporation.
 *
 *  $Copyright-End$
 */

package org.jgroups.blocks;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.executor.ExecutionRunner;
import org.jgroups.blocks.executor.ExecutionService;
import org.jgroups.protocols.CENTRAL_EXECUTOR;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests {@link org.jgroups.blocks.executor.ExecutionService}
 * @author wburns
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class ExecutingServiceTest extends ChannelTestBase {
    protected JChannel c1, c2, c3;
    protected ExecutionService e1, e2, e3;
    protected ExecutionRunner er1, er2, er3;
    
    @BeforeClass
    protected void init() throws Exception {
        c1=createChannel(true, 3, "A");
        addExecutingProtocol(c1);
        er1=new ExecutionRunner(c1);
        c1.connect("ExecutionServiceTest");

        c2=createChannel(c1, "B");
        er2=new ExecutionRunner(c2);
        c2.connect("ExecutionServiceTest");

        c3=createChannel(c1, "C");
        er3=new ExecutionRunner(c3);
        c3.connect("ExecutionServiceTest");
    }
    
    @AfterClass
    protected void cleanup() {
        Util.close(c3,c2,c1);
    }
    
    @BeforeMethod
    protected void createExecutors() {
        e1=new ExecutionService(c1);
        e2=new ExecutionService(c2);
        e3=new ExecutionService(c3);
    }

    protected static class SimpleCallable<V> implements Callable<V> {
        final V _object;
        
        public SimpleCallable(V object) {
            _object = object;
        }

        @Override
        public V call() throws Exception {
            return _object;
        }

        // @see java.lang.Object#toString()
        @Override
        public String toString() {
            return "SimpleCallable [value=" + _object + "]";
        }
    }
    
    protected static class SimpleSerializableCallable<V> implements Callable<V>, Serializable {
        private static final long serialVersionUID = -3516929972908331514L;
        
        final V _object;
        
        public SimpleSerializableCallable(V object) {
            _object = object;
        }

        @Override
        public V call() throws Exception {
            return _object;
        }

        // @see java.lang.Object#toString()
        @Override
        public String toString() {
            return "SimpleSerializableCallable [value=" + _object + "]";
        }
    }
    
    @Test
    public void testSimpleSerializableCallableSubmit() 
            throws InterruptedException, ExecutionException, TimeoutException {
        Long value = Long.valueOf(100);
        Callable<Long> callable = new SimpleSerializableCallable<Long>(value);
        Thread consumer = new Thread(er2);
        consumer.start();
        NotifyingFuture<Long> future = e1.submit(callable);
        Long returnValue = future.get(10L, TimeUnit.SECONDS);
        // We try to stop the thread.
        consumer.interrupt();
        assert value == returnValue : "The value returned doesn't match";
        
        consumer.join(2000);
        assert !consumer.isAlive() : "Consumer did not stop correctly";
    }
    
    protected void addExecutingProtocol(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol lockprot = new CENTRAL_EXECUTOR();
        lockprot.setLevel("trace");
        stack.insertProtocolAtTop(lockprot);
    }
}
