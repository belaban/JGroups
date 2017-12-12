package org.jgroups.util;

import org.testng.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Listener generating XML output suitable to be processed by JUnitReport.
 * Copied from TestNG (www.testng.org) and modified
 * 
 * @author Bela Ban
 */
public class JUnitXMLReporter implements ITestListener, IConfigurationListener2 {
    protected String output_dir=null;

    protected static final String XML_DEF="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>";
    protected static final String CDATA="![CDATA[";
    protected static final String LT="&lt;";
    protected static final String GT="&gt;";
    protected static final String SYSTEM_OUT="system-out";
    protected static final String SYSTEM_ERR="system-err";
    protected static final String TESTS="tests.data";
    protected static final String STDOUT="stdout.txt";
    protected static final String STDERR="stderr.txt";

    protected PrintStream old_stdout=System.out;
    protected PrintStream old_stderr=System.err;

    protected final ConcurrentMap<Class<?>, DataOutputStream> tests=new ConcurrentHashMap<>(100);

    public static final InheritableThreadLocal<PrintStream>   stdout=new InheritableThreadLocal<>();
    public static final InheritableThreadLocal<PrintStream>   stderr=new InheritableThreadLocal<>();



    /** Invoked at the start of the test, before any of the classes in the test are run */
    public void onStart(ITestContext context) {
        output_dir=context.getOutputDirectory();
        // Uncomment to delete dir created by previous run of this testsuite
        File dir=new File(output_dir);
        if(dir.exists())
            deleteContents(dir);

        try {
            System.setOut(new MyOutput(1));
            System.setErr(new MyOutput(2));
        }
        catch(FileNotFoundException e) {
        }
    }

    /** Invoked after all test classes in this test have been run */
    public void onFinish(ITestContext context) {
        try {
            tests.values().forEach(Util::close);
            tests.clear();
            generateReports();
        }
        catch(IOException e) {
            error(e.toString());
        }
        finally {
            System.setOut(old_stdout);
            System.setErr(old_stderr);
        }
    }


    /* Invoked at the start of each test method in a test class */
    public void onTestStart(ITestResult result) {
        setupStreams(result, true);
    }


    /** Invoked each time a test method succeeds */
    public void onTestSuccess(ITestResult tr) {
        onTestCompleted(tr, "OK:   ", old_stdout);
    }


    public void onTestFailedButWithinSuccessPercentage(ITestResult tr) {
        onTestCompleted(tr, "OK:   ",old_stdout);
    }

    /** Invoked each time a test method fails */
    public void onTestFailure(ITestResult tr) {
        onTestCompleted(tr, "FAIL: ",old_stderr);
    }

    /** Invoked each time a test method is skipped */
    public void onTestSkipped(ITestResult tr) {
        onTestCompleted(tr, "SKIP: ",old_stderr);
    }

    public void beforeConfiguration(ITestResult tr) {
        setupStreams(tr, false);
    }

    public void onConfigurationSuccess(ITestResult tr) {
        closeStreams();
    }

    public void onConfigurationFailure(ITestResult tr) {
        error("failed config: " + tr.getThrowable());
        onTestCompleted(tr, "FAIL: ", old_stderr);
    }

    public void onConfigurationSkip(ITestResult tr) {
        closeStreams();
    }


    protected void onTestCompleted(ITestResult tr, String message, PrintStream out) {
        Class<?> real_class=tr.getTestClass().getRealClass();
        addTest(real_class,tr);
        print(out,message,real_class.getName(),getMethodName(tr));
        closeStreams();
    }

    protected void setupStreams(ITestResult result, boolean printMethodName) {
        String test_name=result.getTestClass().getName();
        File dir=new File(output_dir + File.separator + test_name);
        if(!dir.exists())
            dir.mkdirs();
        File _tests=new File(dir, TESTS), _stdout=new File(dir, STDOUT), _stderr=new File(dir, STDERR);
        try {
            Class<?> clazz=result.getTestClass().getRealClass();
            if(!tests.containsKey(clazz)) {
                DataOutputStream output=new DataOutputStream(new FileOutputStream(_tests,true));
                DataOutputStream tmp=tests.putIfAbsent(clazz, output);
                if(tmp != null) {
                    Util.close(output);
                    output=tmp;
                }
            }
            
            if(stdout.get() == null)
                stdout.set(new PrintStream(new FileOutputStream(_stdout, true)));
            if(stderr.get() == null)
                stderr.set(new PrintStream(new FileOutputStream(_stderr, true)));
            if(printMethodName)
                stdout.get().println("\n\n------------- " + getMethodName(result) + " -----------");
        }
        catch(IOException e) {
            error(e.toString());
        }
    }

    protected static void closeStreams() {
        Util.close(stdout.get());
        stdout.set(null);
        Util.close(stderr.get());
        stderr.set(null);
    }

    protected static void print(PrintStream out, String msg, String classname, String method_name) {
        out.println(msg + "[" + Thread.currentThread().getId() + "] " + classname + "." + method_name + "()");
    }

    protected void error(String msg) {
        old_stderr.println(msg);
    }

    protected void println(String msg) {
        old_stdout.println(msg);
    }

    protected void addTest(Class<?> clazz, ITestResult result) {
        try {
            TestCase test_case=new TestCase(result.getStatus(), clazz.getName(), getMethodName(result),
                                            result.getStartMillis(), result.getEndMillis());
            switch(result.getStatus()) {
                case ITestResult.FAILURE:
                case ITestResult.SKIP:
                    Throwable ex=result.getThrowable();
                    if(ex != null) {
                        String failure_type=ex.getClass().getName();
                        String failure_msg=ex.getMessage();
                        String stack_trace=printException(ex);
                        test_case.setFailure(failure_type, failure_msg, stack_trace);
                    }
                    else
                        test_case.setFailure("exception", "SKIPPED", null);
                    break;
            }

            synchronized(this) { // handle concurrent access by different threads, if test methods are run in parallel
                DataOutputStream output=tests.get(clazz);
                test_case.writeTo(output);
            }
        }
        catch(Exception e) {
            error(e.toString());
        }
    }

    protected static String getMethodName(ITestResult tr) {
        String method_name=tr.getMethod().getMethodName();
        Object[] params=tr.getParameters();
        if(params != null && params.length > 0) {
            String tmp=null;
            if(params[0] instanceof Class<?>)
                tmp=((Class<?>)params[0]).getSimpleName();
            else if(params[0] != null)
                tmp=params[0].getClass().getSimpleName();
            if(tmp != null)
                method_name=method_name + "-" + tmp;
        }
        return method_name;
    }


    /** Generate the XML report from all the test results */
    protected void generateReports() throws IOException {
        File root_dir=new File(output_dir);
        if(!root_dir.exists())
            throw new IOException(root_dir + " not found");
        File[] subdirs=root_dir.listFiles(File::isDirectory);
        if(subdirs != null) {
            for(File dir: subdirs) {
                try {
                    process(dir);
                }
                catch(Throwable e) {
                    error(e.toString());
                }
            }
        }
    }


    protected static void process(File dir) throws IOException {
        File file=new File(dir, TESTS);
        if(!file.exists())
            throw new IOException(file + " not found");
        List<TestCase> test_cases=new ArrayList<>();
        DataInputStream input=new DataInputStream(new FileInputStream(file));
        try {
            for(;;) {
                TestCase test_case=new TestCase();
                try {
                    test_case.readFrom(input);
                    test_cases.add(test_case);
                }
                catch(Exception e) {
                    break;
                }
            }
        }
        finally {
            Util.close(input);
        }

        if(test_cases.isEmpty())
            return;

        Reader stdout_reader=null, stderr_reader=null;
        File tmp=new File(dir, STDOUT);
        if(tmp.exists() && tmp.length() > 0)
            stdout_reader=new FileReader(tmp);

        tmp=new File(dir, STDERR);
        if(tmp.exists() && tmp.length() > 0)
            stderr_reader=new FileReader(tmp);
        File parent=dir.getParentFile();
        File xml_file=new File(parent, "TESTS-" + dir.getName() + "-" + parent.getName() + ".xml");
        Writer out=new FileWriter(xml_file);
        String classname=dir.getName();
        String suffix=parent.getName();
        if(suffix != null && !suffix.isEmpty())
            classname=classname + "-" + suffix;
        try {
            generateReport(out, classname, test_cases, stdout_reader, stderr_reader);
        }
        finally {
            Util.close(out, stdout_reader, stderr_reader);
        }
    }



    /** Generate the XML report from all the test results */
    protected static void generateReport(Writer out, String classname, List<TestCase> results,
                                         Reader stdout, Reader stderr) throws IOException {
        int num_failures=getFailures(results);
        int num_skips=getSkips(results);
        int num_errors=getErrors(results);
        long total_time=getTotalTime(results);

        try {
            out.write(XML_DEF + "\n");

            out.write("\n<testsuite "
                        + "name=\""   + classname + "\" "
                        + "tests=\""  + results.size() + "\" "
                        + "failures=\"" + num_failures + "\" "
                        + "errors=\"" + num_errors + "\" "
                        + "skips=\""  + num_skips + "\" "
                        + "time=\""    + (total_time / 1000.0) + "\">");

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

            for(TestCase result: results) {
                if(result == null)
                    continue;

                try {
                    writeTestCase(out,result);
                }
                catch(Throwable t) {
                    t.printStackTrace();
                }
            }

            if(stdout != null)
                writeOutput(1, stdout, out);
            if(stderr != null)
                writeOutput(2, stderr, out);
        }
        finally {
            out.write("\n</testsuite>\n");
        }
    }


    protected static void writeTestCase(Writer out, TestCase result) throws IOException {
        long time=result.stop_time - result.start_time;
        // StringBuilder sb=new StringBuilder();
        out.write("\n    <testcase classname=\"" + result.classname);
        out.write("\"  name=\"" + result.name + "\" time=\"" + (time / 1000.0) + "\">");

        switch(result.status) {
            case ITestResult.FAILURE:
                String failure=writeFailure("failure", result.failure_type, result.failure_msg, result.stack_trace);
                if(failure != null)
                    out.write(failure);
                break;
            case ITestResult.SKIP:
                failure=writeFailure("error", result.failure_type, result.failure_msg, result.stack_trace);
                if(failure != null)
                    out.write(failure);
                break;
        }
        out.write("\n    </testcase>\n");
    }

    protected static void writeOutput(int type, Reader in, Writer out) throws IOException {
        out.write("\n<" + (type == 2? SYSTEM_ERR : SYSTEM_OUT) + "><" + CDATA + "\n");
        copy(in, out);
        out.write("\n]]>");
        out.write("\n</" + (type == 2? SYSTEM_ERR : SYSTEM_OUT) + ">");
    }


    protected static String getStatus(TestCase tr) {
        switch(tr.status) {
            case ITestResult.SUCCESS:
            case ITestResult.SUCCESS_PERCENTAGE_FAILURE:
                return "OK";
            case ITestResult.FAILURE: return "FAIL";
            case ITestResult.SKIP:    return "SKIP";
            default:                  return "UNKNOWN";
        }
    }

    protected static String writeFailure(String type, String failure_type, String failure_msg, String stack_trace) throws IOException {
        StringBuilder sb=new StringBuilder();
        sb.append("\n        <" + type + " type=\"").append(failure_type);
        sb.append("\" message=\"" + escape(failure_msg) + "\">");
        if(stack_trace != null)
            sb.append(stack_trace);
        sb.append("\n        </" + type + ">");
        return sb.toString();
    }

    protected static String printException(Throwable ex) throws IOException {
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

    protected static String escape(String message) {
        return message != null? message.replaceAll("<", LT).replaceAll(">", GT) : message;
    }

    public static long getTotalTime(Collection<TestCase> results) {
        long start=0, stop=0;
        for(TestCase result: results) {
            if(result == null)
                continue;
            long tmp_start=result.start_time, tmp_stop=result.stop_time;
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

    public static int getFailures(Collection<TestCase> results) {
        int retval=0;
        for(TestCase result: results) {
            if(result != null && result.status == ITestResult.FAILURE)
                retval++;
        }
        return retval;
    }

    public static int getErrors(Collection<TestCase> results) {
        int retval=0;
        for(TestCase result: results) {
            if(result != null && result.status != ITestResult.SUCCESS
              && result.status != ITestResult.SUCCESS_PERCENTAGE_FAILURE
              && result.status != ITestResult.FAILURE)
                retval++;
        }
        return retval;
    }

    public static int getSkips(Collection<TestCase> results) {
        int retval=0;
        for(TestCase result: results) {
            if(result != null && result.status == ITestResult.SKIP)
                retval++;
        }
        return retval;
    }

    /** Deletes all files and dirs in dir, but not dir itself */
    protected static void deleteContents(File dir) {
        File[] contents=dir.listFiles();
        if(contents != null) {
            for(File file: contents) {
                if(file.isDirectory()) {
                    deleteContents(file);
                    file.delete();
                }
                else
                    file.delete();
            }
        }
    }

    /** Copies the contents of in into out */
    protected static int copy(Reader in, Writer out) {
        int count=0;

        char[] buf=new char[1024];

        while(true) {
            try {
                int num=in.read(buf, 0, buf.length);
                if(num == -1)
                    break;
                out.write(buf, 0, num);
                count+=num;
            }
            catch(IOException e) {
                break;
            }
        }

        return count;
    }

    protected static class MyOutput extends PrintStream {
        private static final String TMPFILE_NAME;
        static {
            if (Util.checkForWindows()) {
                TMPFILE_NAME = System.getProperty("java.io.tmpdir") + "\\" + "tmp.txt";
            } else {
                TMPFILE_NAME = System.getProperty("java.io.tmpdir") + "/" + "tmp.txt";
            }
        }
        final int type;

        public MyOutput(int type) throws FileNotFoundException {
            super(TMPFILE_NAME); // dummy name
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
            append(String.valueOf(b), false) ;
        }

        public void flush() {
            append("", true);
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

        protected synchronized void append(String x, boolean newline) {
            PrintStream tmp=type == 1? stdout.get() : stderr.get();
            if(tmp == null)
                return;
            if(newline)
                tmp.println(x);
            else
                tmp.print(x);
        }
    }

    public static void main(String[] args) throws IOException {
        JUnitXMLReporter reporter=new JUnitXMLReporter();
        reporter.output_dir="/home/bela/JGroups/tmp/test-results/xml/udp";
        reporter.generateReports();
    }


    public static class TestCase implements Streamable {
        protected int    status;
        protected String classname;
        protected String name;
        protected long   start_time;
        protected long   stop_time;
        protected String failure_type; // null: no failure, class of the Throwable
        protected String failure_msg;  // result of Throwable.getMessage()
        protected String stack_trace;


        public TestCase() { // needed for externalization
        }

        public TestCase(int status, String classname, String name, long start_time, long stop_time) {
            this.status=status;
            this.classname=classname;
            this.name=name;
            this.start_time=start_time;
            this.stop_time=stop_time;
        }

        public void setFailure(String failure_type, String failure_msg, String stack_trace) {
            this.failure_type=failure_type;
            this.failure_msg=failure_msg;
            this.stack_trace=stack_trace;
        }

        public long getTime() {return stop_time-start_time;}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(statusToString(status)).append(" ").append(classname).append(".").append(name).append(" in ")
              .append(getTime()).append(" ms");
            if(failure_type != null)
                sb.append("\n" + failure_type).append(" msg=" + failure_msg).append("\n").append(stack_trace);
            return sb.toString();
        }

        protected static String statusToString(int status) {
            switch(status) {
                case ITestResult.SUCCESS:
                case ITestResult.SUCCESS_PERCENTAGE_FAILURE:
                    return "OK";
                case ITestResult.FAILURE: return "FAIL";
                case ITestResult.SKIP:    return "SKIP";
                default:                  return "N/A";
            }
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(status);
            Bits.writeString(classname,out);
            Bits.writeString(name,out);
            out.writeLong(start_time);
            out.writeLong(stop_time);
            Bits.writeString(failure_type,out);
            Bits.writeString(failure_msg,out);
            Bits.writeString(stack_trace,out);
        }

        public void readFrom(DataInput in) throws IOException {
            status=in.readInt();
            classname=Bits.readString(in);
            name=Bits.readString(in);
            start_time=in.readLong();
            stop_time=in.readLong();
            failure_type=Bits.readString(in);
            failure_msg=Bits.readString(in);
            stack_trace=Bits.readString(in);
        }
    }

}
