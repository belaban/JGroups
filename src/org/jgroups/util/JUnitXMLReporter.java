package org.jgroups.util;

import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.TestListenerAdapter;
import org.testng.annotations.Test;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Listener generating XML output suitable to be processed by JUnitReport.
 * Copied from TestNG (www.testng.org) and modified
 * 
 * @author Bela Ban
 */
public class JUnitXMLReporter extends TestListenerAdapter implements IInvokedMethodListener {
    private String output_dir=null;
    private String suffix=null;

    private static final String SUFFIX="test.suffix";
    private static final String XML_DEF="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>";
    private static final String CDATA="![CDATA[";
    private static final String LT="&lt;";
    private static final String GT="&gt;";
    private static final String SYSTEM_OUT="system-out";
    private static final String SYSTEM_ERR="system-err";

    private PrintStream old_stdout=System.out;
    private PrintStream old_stderr=System.err;

    private final ConcurrentMap<Class<?>,Collection<ITestResult>> classes=new ConcurrentHashMap<Class<?>,Collection<ITestResult>>();

    /** Map to keep systemout and systemerr associated with a class */
    private final ConcurrentMap<Class<?>,Tuple<StringBuffer,StringBuffer>> outputs=new ConcurrentHashMap<Class<?>,Tuple<StringBuffer,StringBuffer>>();

    // Keeps track of already gnerated reports, so we don't generate the same report multiple times
    protected final Set<Class<?>> generated_reports=new HashSet<Class<?>>();

    public static InheritableThreadLocal<Class<?>> local=new InheritableThreadLocal<Class<?>>();

    class MyOutput extends PrintStream {
        final int type; 

        public MyOutput(String fileName,int type) throws FileNotFoundException {
            super(fileName);
            this.type=type;
            if(type != 1 && type != 2)
                throw new IllegalArgumentException("index has to be 1 or 2");
        }
        
        public void write(final byte[] b) {
        	String s = new String(b) ;
        	append(s.trim(), false) ;
        }

        public void write(final byte[] b, final int off, final int len) {
        	String s = new String(b, off, len) ;
        	append(s.trim(), false) ;
        }
        
        public void write(final int b) {
        	Integer i = new Integer(b) ;
        	append(i.toString(), false) ;
        }

        public void println(String s) {
            append(s, true);
        }

        public void print(String s) {
            append(s, false);
        }

        public void print(Object obj) {
            if(obj != null)
                append(obj.toString(), false);
            else
                append("null", false);
        }

        public void println(Object obj) {
            if(obj != null)
                append(obj.toString(), true);
            else
                append("null", true);
        }

        private synchronized void append(String x, boolean newline) {
            Class<?> clazz=local.get();
            if(clazz != null) {
                Tuple<StringBuffer,StringBuffer> tuple=outputs.get(clazz);
                if(tuple != null) {
                    StringBuffer sb=type == 1? tuple.getVal1() : tuple.getVal2();
                    if(sb.length() == 0) {
                        sb.append("\n" + clazz.getName() + ":");
                    }
                    sb.append("\n").append(x);
                    return;
                }
            }
            PrintStream stream=type == 2? old_stderr : old_stdout;
            if(newline)
                stream.println(x);
            else
                stream.print(x);
        }
    }

    /** Invoked before any method (configuration or test) is invoked */
    public void beforeInvocation(IInvokedMethod method, ITestResult tr) {
        Class<?> real_class=tr.getTestClass().getRealClass();

        local.set(real_class);

        Collection<ITestResult> results=classes.get(real_class);
        if(results == null) {
            results=new LinkedList<ITestResult>();
            classes.putIfAbsent(real_class,results);
        }

        outputs.putIfAbsent(real_class, new Tuple<StringBuffer,StringBuffer>(new StringBuffer(), new StringBuffer())) ;
    }

    /** Invoked after any method (configuration or test) is invoked */
    public void afterInvocation(IInvokedMethod method, ITestResult tr) {

    }

    /* Moved code from onTestStart() to beforeInvocation() to avoid output leaks (JGRP-850) */ 
    public void onTestStart(ITestResult result) {
        
    }

    /** Invoked each time a test succeeds */
    public void onTestSuccess(ITestResult tr) {
        Class<?> real_class=tr.getTestClass().getRealClass();
        addTest(real_class, tr);
        print(old_stdout, "OK:   ", real_class.getName(), tr.getName());
    }

    public void onTestFailedButWithinSuccessPercentage(ITestResult tr) {
        Class<?> real_class=tr.getTestClass().getRealClass();
        addTest(tr.getTestClass().getRealClass(), tr);
        print(old_stdout, "OK:   ", real_class.getName(), tr.getName());
    }

    /**
     * Invoked each time a test fails.
     */
    public void onTestFailure(ITestResult tr) {
        Class<?> real_class=tr.getTestClass().getRealClass();
        addTest(tr.getTestClass().getRealClass(), tr);
        print(old_stderr, "FAIL: ", real_class.getName(), tr.getName());
    }

    /**
     * Invoked each time a test is skipped.
     */
    public void onTestSkipped(ITestResult tr) {
        Class<?> real_class=tr.getTestClass().getRealClass();
        addTest(tr.getTestClass().getRealClass(), tr);
        print(old_stdout, "SKIP: ", real_class.getName(), tr.getName());
    }

    private static void print(PrintStream out, String msg, String classname, String method_name) {
        out.println(msg + "["
                    + Thread.currentThread().getId()
                    + "] "
                    + classname
                    + "."
                    + method_name
                    + "()");
    }

    private void addTest(Class<?> clazz, ITestResult result) {
        Collection<ITestResult> results=classes.get(clazz);
        if(results == null) {
            results=new ConcurrentLinkedQueue<ITestResult>();
            Collection<ITestResult> tmp=classes.putIfAbsent(clazz,results);
            if(tmp != null)
                results=tmp;
        }
        results.add(result);
        
        ITestNGMethod[] testMethods=result.getMethod().getTestClass().getTestMethods();
        int enabledCount = enabledMethods(testMethods);
        boolean allTestsInClassCompleted = results.size() >= enabledCount;
        if(allTestsInClassCompleted) {
            boolean do_generate=false;
            synchronized(generated_reports) {
                do_generate=generated_reports.add(clazz);
            }
            try {
                if(do_generate)
                    generateReport(clazz, results);
            }
            catch(IOException e) {
                print(old_stderr, "Failed generating report: ", clazz.getName(), "");
            }
        }                   
    }

    private static int enabledMethods(ITestNGMethod[] testMethods) {
        int count = testMethods.length;
        for(ITestNGMethod testNGMethod:testMethods) {
            Method m = testNGMethod.getConstructorOrMethod().getMethod();
            if(m != null && m.isAnnotationPresent(Test.class)){
              Test annotation=m.getAnnotation(Test.class);  
              if(!annotation.enabled()){
                  count --;
              }
            }
        }
        return count;
    }

    /**
     * Invoked after the test class is instantiated and before any configuration
     * method is called.
     */
    public void onStart(ITestContext context) {
        suffix=System.getProperty(SUFFIX);
        if(suffix != null)
            suffix=suffix.trim();
        output_dir=context.getOutputDirectory(); // + File.separator + context.getName() + suffix + ".xml";

        try {
            System.setOut(new MyOutput("/tmp/tmp.txt", 1));
        }
        catch(FileNotFoundException e) {
        }

        try {
            System.setErr(new MyOutput("/tmp/tmp.txt", 2));
        }
        catch(FileNotFoundException e) {
        }
    }

    /**
     * Invoked after all the tests have run and all their Configuration methods
     * have been called.
     */
    public void onFinish(ITestContext context) {
        System.setOut(old_stdout);
        System.setErr(old_stderr);
    }
    
    /**
     * generate the XML report given what we know from all the test results
     */
    protected void generateReport(Class<?> clazz, Collection<ITestResult> results) throws IOException {

        int num_failures=getFailures(results);
        int num_skips=getSkips(results);
        int num_errors=getErrors(results);
        long total_time=getTotalTime(results);

        String file_name=output_dir + File.separator + "TEST-" + clazz.getName();
        if(suffix != null)
            file_name=file_name + "-" + suffix;
        file_name=file_name + ".xml";
        Writer out=new FileWriter(file_name, false); // don't append, overwrite
        try {
            out.write(XML_DEF + "\n");

            out.write("\n<testsuite " + " failures=\""
                      + num_failures
                      + "\" errors=\""
                      + num_errors
                      + "\" skips=\""
                      + num_skips
                      + "\" name=\""
                      + clazz.getName());
            if(suffix != null)
                out.write(" (" + suffix + ")");
            out.write("\" tests=\"" + results.size() + "\" time=\"" + (total_time / 1000.0) + "\">");

            out.write("\n<properties>");
            Properties props=System.getProperties();

            for(Map.Entry<Object,Object> tmp: props.entrySet()) {
                out.write("\n    <property name=\"" + tmp.getKey()
                          + "\""
                          + " value=\""
                          + tmp.getValue()
                          + "\"/>");
            }
            out.write("\n</properties>\n");

            for(ITestResult result: results) {
                if(result == null)
                    continue;

                try {
                    String testcase=writeTestCase(result,clazz,suffix);
                    out.write(testcase);
                }
                catch(Throwable t) {
                    t.printStackTrace();
                }
            }

            Tuple<StringBuffer,StringBuffer> stdout=outputs.get(clazz);
            if(stdout != null) {
                StringBuffer system_out=stdout.getVal1();
                StringBuffer system_err=stdout.getVal2();
                writeOutput(out, system_out.toString(), 1);
                out.write("\n");
                writeOutput(out, system_err.toString(), 2);
            }
        }
        finally {
            out.write("\n</testsuite>\n");
            out.close();
        }
    }

    protected static String writeTestCase(ITestResult result, Class<?> clazz, String suffix) throws IOException {
        if(result == null)
            return null;
        long time=result.getEndMillis() - result.getStartMillis();
        StringBuilder sb=new StringBuilder();
        sb.append("\n    <testcase classname=\"" + clazz.getName());
        if(suffix != null)
            sb.append(" (" + suffix + ")");
        sb.append("\" name=\"" + result.getMethod().getMethodName() + "\" time=\"" + (time / 1000.0) + "\">");

        Throwable ex=result.getThrowable();

        switch(result.getStatus()) {
            case ITestResult.SUCCESS:
            case ITestResult.SUCCESS_PERCENTAGE_FAILURE:
            case ITestResult.STARTED:
                break;
            case ITestResult.FAILURE:
                String failure=writeFailure("failure",
                                            result.getMethod().getConstructorOrMethod().getMethod(),
                                            ex,
                                            "exception");
                if(failure != null)
                    sb.append(failure);
                break;
            case ITestResult.SKIP:
                failure=writeFailure("error", result.getMethod().getConstructorOrMethod().getMethod(), ex, "SKIPPED");
                if(failure != null)
                    sb.append(failure);
                break;
            default:
                failure=writeFailure("error", result.getMethod().getConstructorOrMethod().getMethod(), ex, "exception");
                if(failure != null)
                    sb.append(failure);
        }

        sb.append("\n    </testcase>\n");
        return sb.toString();
    }

    private static void writeOutput(Writer out, String s, int type) throws IOException {
        if(s != null && s.length() > 0) {
            out.write("\n<" + (type == 2? SYSTEM_ERR : SYSTEM_OUT) + "><" + CDATA + "\n");
            out.write(s);
            out.write("\n]]>");
            out.write("\n</" + (type == 2? SYSTEM_ERR : SYSTEM_OUT) + ">");
        }
    }

    private static String writeFailure(String type, Method method, Throwable ex, String msg) throws IOException {
        Test annotation=method != null? method.getAnnotation(Test.class) : null;
        if(annotation != null && ex != null) {
            Class<?>[] expected_exceptions=annotation.expectedExceptions();
            for(int i=0;i < expected_exceptions.length;i++) {
                Class<?> expected_exception=expected_exceptions[i];
                if(expected_exception.equals(ex.getClass())) {
                    return null;
                }
            }
        }

        StringBuilder sb=new StringBuilder();
        sb.append("\n        <" + type + " type=\"");
        if(ex != null) {
            sb.append(ex.getClass().getName() + "\" message=\"" + escape(ex.getMessage()) + "\">");
            String ex_str=printException(ex);
            if(ex_str != null)
                sb.append(ex_str);
        }
        else
            sb.append("exception\" message=\"" + msg + "\">");
        sb.append("\n        </" + type + ">");
        return sb.toString();
    }

    private static String printException(Throwable ex) throws IOException {
        if(ex == null)
            return null;
        StackTraceElement[] stack_trace=ex.getStackTrace();
        StringBuilder sb=new StringBuilder();
        sb.append("\n<" + CDATA + "\n");
        sb.append(ex.getClass().getName() + " \n");
        for(int i=0;i < stack_trace.length;i++) {
            StackTraceElement frame=stack_trace[i];
            sb.append("at " + frame.toString() + " \n");
        }
        sb.append("\n]]>");
        return sb.toString();
    }

    private static String escape(String message) {
        return message != null? message.replaceAll("<", LT).replaceAll(">", GT) : message;
    }

    private static long getTotalTime(Collection<ITestResult> results) {
        long start=0, stop=0;
        for(ITestResult result:results) {
            if(result == null)
                continue;
            long tmp_start=result.getStartMillis(), tmp_stop=result.getEndMillis();
            if(start == 0)
                start=tmp_start;
            else {
                start=Math.min(start, tmp_start);
            }

            if(stop == 0)
                stop=tmp_stop;
            else {
                stop=Math.max(stop, tmp_stop);
            }
        }
        return stop - start;
    }

    private static int getFailures(Collection<ITestResult> results) {
        int retval=0;
        for(ITestResult result:results) {
            if(result != null && result.getStatus() == ITestResult.FAILURE)
                retval++;
        }
        return retval;
    }

    private static int getErrors(Collection<ITestResult> results) {
        int retval=0;
        for(ITestResult result:results) {
            if(result != null && result.getStatus() != ITestResult.SUCCESS
               && result.getStatus() != ITestResult.SUCCESS_PERCENTAGE_FAILURE
               && result.getStatus() != ITestResult.FAILURE)
                retval++;
        }
        return retval;
    }

    private static int getSkips(Collection<ITestResult> results) {
        int retval=0;
        for(ITestResult result:results) {
            if(result != null && result.getStatus() == ITestResult.SKIP)
                retval++;
        }
        return retval;
    }

}
