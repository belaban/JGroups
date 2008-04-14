package org.jgroups.util;

import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Listener generating XML output suitable to be processed by JUnitReport. Copied from TestNG (www.testng.org) and
 * modified
 * @author Bela Ban
 * @version $Id: JUnitXMLReporter.java,v 1.4 2008/04/14 06:40:55 belaban Exp $
 */
public class JUnitXMLReporter extends TestListenerAdapter {
    private String output_dir=null;
    private String suffix=null;

    private List<ITestResult> m_configIssues=Collections.synchronizedList(new ArrayList<ITestResult>());
    private static final String SUFFIX="test.suffix";
    private static final String XML_DEF="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>";
    private static final String CDATA="![CDATA[";
    private static final String LT="&lt;";
    private static final String GT="&gt;";


    private Map<Class, List<ITestResult>> classes=new HashMap<Class,List<ITestResult>>();


    /** Invoked each time a test succeeds */
    public void onTestSuccess(ITestResult tr) {
        addTest(tr.getTestClass().getRealClass(), tr);
    }

    public void onTestFailedButWithinSuccessPercentage(ITestResult tr) {
        addTest(tr.getTestClass().getRealClass(), tr);
    }

    private void addTest(Class clazz, ITestResult result) {
        List<ITestResult> results=classes.get(clazz);
        if(results == null) {
            results=new LinkedList<ITestResult>();
            classes.put(clazz, results);
        }
        results.add(result);
    }

    /**
     * Invoked each time a test fails.
     */
    public void onTestFailure(ITestResult tr) {
        addTest(tr.getTestClass().getRealClass(), tr);
    }

    /**
     * Invoked each time a test is skipped.
     */
    public void onTestSkipped(ITestResult tr) {
        addTest(tr.getTestClass().getRealClass(), tr);
    }

    /**
     * Invoked after the test class is instantiated and before any configuration method is called.
     */
    public void onStart(ITestContext context) {
        suffix=System.getProperty(SUFFIX);
        if(suffix != null)
            suffix=suffix.trim();
        output_dir=context.getOutputDirectory(); // + File.separator + context.getName() + suffix + ".xml";
    }

    /**
     * Invoked after all the tests have run and all their
     * Configuration methods have been called.
     */
    public void onFinish(ITestContext context) {
        try {
            generateReport();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * generate the XML report given what we know from all the test results
     */
    protected void generateReport() throws IOException {
        for(Map.Entry<Class,List<ITestResult>> entry: classes.entrySet()) {
            Class clazz=entry.getKey();
            List<ITestResult> results=entry.getValue();

            int num_failures=getFailures(results);
            int num_skips=getSkips(results);
            int num_errors=getErrors(results);
            long total_time=getTotalTime(results);

            String file_name=output_dir + File.separator + "TEST-" + clazz.getName();
            if(suffix != null)
                file_name=file_name + "-" + suffix;
            file_name=file_name + ".xml";
            FileWriter out=new FileWriter(file_name, false); // don't append, overwrite
            try {
                out.write(XML_DEF + "\n");

                out.write("\n<testsuite " +
                        " failures=\"" + num_failures +
                        "\" errors=\"" + num_errors +
                        "\" skips=\"" + num_skips +
                        "\" name=\"" + clazz.getName());
                if(suffix != null)
                    out.write(" (" + suffix + ")");
                out.write("\" tests=\"" + results.size() + "\" time=\"" + (total_time / 1000.0) + "\">");

                out.write("\n<properties>");
                Properties props=System.getProperties();

                for(Map.Entry<Object,Object> tmp: props.entrySet()) {
                    out.write("\n    <property name=\"" + tmp.getKey() + "\"" +
                    " value=\"" + tmp.getValue() + "\"/>");
                }
                out.write("\n</properties>\n");


                for(ITestResult result: results) {
                    long time=result.getEndMillis() - result.getStartMillis();
                    out.write("\n    <testcase classname=\"" + clazz.getName());
                    if(suffix != null)
                        out.write(" (" + suffix + ")");
                    out.write("\" name=\"" + result.getMethod().getMethodName() +
                            "\" time=\"" + (time/1000.0) + "\">");

                    Throwable ex=result.getThrowable();

                    if(result.getStatus() != ITestResult.SUCCESS && result.getStatus() != ITestResult.SUCCESS_PERCENTAGE_FAILURE) {
                        System.out.println("FAIL ("
                                + result.getStatus() + "): " + result.getMethod().getMethod() + ", ex=" + ex);
                    }

                    switch(result.getStatus()) {
                        case ITestResult.SUCCESS:
                            case ITestResult.SUCCESS_PERCENTAGE_FAILURE:
                                break;
                        case ITestResult.FAILURE:
                            writeFailure("failure", result.getMethod().getMethod(), ex, "exception", out);
                            break;
                        case ITestResult.SKIP:
                            writeFailure("error", result.getMethod().getMethod(), ex, "SKIPPED", out);
                            break;
                        default:
                            writeFailure("error", result.getMethod().getMethod(), ex, "exception", out);
                    }

                    out.write("\n</testcase>");
                }

                out.write("\n</testsuite>\n");
            }
            finally {
                out.close();
            }
        }

    }



    private static void writeFailure(String type, Method method, Throwable ex, String msg, FileWriter out) throws IOException {
        Test annotation=method.getAnnotation(Test.class);
        if(annotation != null && ex != null) {
            Class[] expected_exceptions=annotation.expectedExceptions();
            for(int i=0; i < expected_exceptions.length; i++) {
                Class expected_exception=expected_exceptions[i];
                if(expected_exception.equals(ex.getClass())) {
                    return;
                }
            }
        }

        out.write("\n<" + type + " type=\"");
        if(ex != null) {
            out.write(ex.getClass().getName() + "\" message=\"" + escape(ex.getMessage()) + "\">");
            printException(ex, out);
        }
        else
            out.write("exception\" message=\"" + msg + "\">");
        out.write("\n</" + type + ">");
    }

    private static void printException(Throwable ex, FileWriter out) throws IOException {
        if(ex == null) return;
        StackTraceElement[] stack_trace=ex.getStackTrace();
        out.write("\n<" + CDATA + "\n");
        out.write(ex.getClass().getName() + " \n");
        for(int i=0; i < stack_trace.length; i++) {
            StackTraceElement frame=stack_trace[i];
            try {
                out.write("at " + frame.toString() + " \n");
            }
            catch(IOException e) {
            }
        }
        out.write("\n]]>");
    }

    private static String escape(String message) {
        return message != null? message.replaceAll("<", LT).replaceAll(">", GT) : message;
    }

    private static long getTotalTime(List<ITestResult> results) {
        long start=0, stop=0;
        for(ITestResult result: results) {
            if(result == null) continue;
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
        return stop-start;
    }

    private static int getFailures(List<ITestResult> results) {
        int retval=0;
        for(ITestResult result: results) {
            if(result != null && result.getStatus() == ITestResult.FAILURE)
                retval++;
        }
        return retval;
    }

    private static int getErrors(List<ITestResult> results) {
        int retval=0;
        for(ITestResult result: results) {
            if(result != null
                    && result.getStatus() != ITestResult.SUCCESS
                    && result.getStatus() != ITestResult.SUCCESS_PERCENTAGE_FAILURE
                    && result.getStatus() != ITestResult.FAILURE)
                retval++;
        }
        return retval;
    }

    private static int getSkips(List<ITestResult> results) {
        int retval=0;
        for(ITestResult result: results) {
            if(result != null && result.getStatus() == ITestResult.SKIP)
                retval++;
        }
        return retval;
    }


    public void onConfigurationFailure(ITestResult itr) {
        m_configIssues.add(itr);
    }

    public void onConfigurationSkip(ITestResult itr) {
        m_configIssues.add(itr);
    }


}
