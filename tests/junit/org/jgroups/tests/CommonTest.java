// $Id: CommonTest.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.tests;


import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import junit.framework.*;
import org.jgroups.log.Trace;



/**
 * Uses the JUnit suite to provide unit and module testing for the
 * org.jgroups.log classes. To run, type "make test".
 *
 * @see <a href="http://www.junit.org">JUnit.org</a>
 * @author Jim Menard
 */
public class CommonTest extends TestCase {

    protected static final int TEST_SOCKET_PORT = 12345;

    protected static final String TMP_OUTPUT_FILE = "Common_test.out";
    protected static final String TMP_ROOT = System.getProperty("tests.tmp.dir") != null?
        System.getProperty("tests.tmp.dir") :
            (System.getProperty("java.io.tmpdir") != null? System.getProperty("java.io.tmpdir") : "/tmp");
    protected static final String GENERATED_TMP_OUTPUT_FILE = 
      TMP_ROOT +"/Module_DEBUG";
    protected static final String GENERATED_METHOD_TMP_OUTPUT_FILE =
	   TMP_ROOT + "/Module.method_FATAL";

    protected File tmpOutput;
    protected File generatedTmpOutput;
    protected File generatedMethodTmpOutput;

    protected String[] expectedDebugResults = {
	"] [DEBUG] Module: module debug",
	"] [FATAL] Module: module fatal",
	"] [DEBUG] Module.method(): module with method name",
	"] [DEBUG] Module.method(arg): module with method name and arg"
    };

    protected String[] expectedFatalResults = {
	"] [FATAL] Module: module fatal",
    };

    protected String[] expectedDefaultResults = {
	"] [DEBUG] Module2: output to default",
    };

    // Lets JUnit use reflection to find all test* methods and add them to the
    // test suite.
    //
    // Actually, as of JUnit version 2.1 even this method is no
    // longer necessary. Junit's TestRunner will automatically find and run
    // all public void methods that start with "test".
    public static Test suite() {
	return new TestSuite(CommonTest.class);
    }

    public void setUp() {
	tmpOutput = new File(TMP_OUTPUT_FILE);
	generatedTmpOutput = new File(GENERATED_TMP_OUTPUT_FILE);
	generatedMethodTmpOutput = new File(GENERATED_METHOD_TMP_OUTPUT_FILE);

	Trace.closeAllOutputs();
	Trace.restoreDefaultOutput();
	Trace.setDefaultOutput(Trace.DEBUG, System.out);
	deleteTempFiles();
    }

    public void tearDown() {
	Trace.closeAllOutputs();
	deleteTempFiles();
    }

    protected void deleteTempFiles() {
	if (tmpOutput.exists()) tmpOutput.delete();
	if (generatedTmpOutput.exists()) generatedTmpOutput.delete();
	if (generatedMethodTmpOutput.exists()) generatedMethodTmpOutput.delete();
    }

    public CommonTest(String name) {
	super(name);
    }

    // Test a bug reported by Bela.
    public void testInnerClasses() {
	try {
	    Trace.setOutput("FD", Trace.DEBUG, TMP_OUTPUT_FILE);
	    Trace.println("FD.PingThread.run()", Trace.WARN, "thread terminated");
	    Trace.println("FD.setProperties()", Trace.FATAL,
			  "property timeout not recognized");
	    Trace.closeAllOutputs();

	    String[] expectedResults = {
		"] [WARN] FD.PingThread.run(): thread terminated",
		"] [FATAL] FD.setProperties(): property timeout not recognized",
	    };
	    compareOutputs(TMP_OUTPUT_FILE, expectedResults);
	}
	catch (IOException e) {
	    fail("testInnerClasses: exception " + e + " thrown");
	}
    }

    public void testBogusModuleName() {
	try {	    
	    Trace.setOutput("method()", Trace.DEBUG, TMP_OUTPUT_FILE);
	    // Trace.setDefaultOutput("/dev/null");
            Trace.closeDefaultOutput();

	    // Messages should go to default output (/dev/null)
	    sendTestMessages();
	    Trace.closeOutput("method()");

	    // Make sure file was never created.
	    File f = new File(TMP_OUTPUT_FILE);
	    assertTrue("testBogusModuleName: file should never be created",
		   !f.exists());
	}
	catch (IOException e) {
	    fail("testBogusModuleName: exception " + e + " thrown");
	}
    }

    public void testNullDefaultOutputOK() {
	//try {
	Trace.closeDefaultOutput();
	Trace.println("Module", Trace.DEBUG, "nowhere to go");
	//}
	//catch (Exception e) {
	//fail("testNullDefaultOutputOK: exception " + e + " thrown");
	//}
    }

    // Change default output to TMP_OUTPUT_FILE and make sure we see output
    // not destined for any other place.
    public void testChangeDefaultOutput() {
	try {
	    Trace.setDefaultOutput(TMP_OUTPUT_FILE);
	    // Trace.setOutput() has not been called; all output should go
	    // to default output.
	    sendTestMessages();
	    Trace.restoreDefaultOutput(); // Closes TMP_OUTPUT_FILE
	    compareOutputs(TMP_OUTPUT_FILE, expectedDebugResults);
	}
	catch (Exception e) {
	    Thread.dumpStack();
	    fail("testChangeDefaultOutput: exception " + e + " thrown");
	}
    }

    // Send messages to default default output (STDOUT).
    public void testDefaultOutput() {
	try {
	    // Reset system out to known file. This doesn't use Trace code;
	    // we are telling the System class to redirect stdout.
	    PrintStream origSystemOut = System.out;
	    PrintStream newSystemOut =
		new PrintStream(new FileOutputStream(TMP_OUTPUT_FILE));
	    System.setOut(newSystemOut);

	    // Since no output assigned for Module2, should go to stdout
	    Trace.println("Module2", Trace.DEBUG, "output to default");

	    // Restore system out
	    System.setOut(origSystemOut);
	    newSystemOut.flush();
	    newSystemOut.close();

	    // Compare contents of log file to array of expected results strings
	    compareOutputs(TMP_OUTPUT_FILE, expectedDefaultResults);
	}
	catch (Exception e) {
	    fail("testDefaultOutput: exception " + e + " thrown");
	}
    }

    public void testDefaultWithLevel() {
	try {
	    Trace.setDefaultOutput(Trace.FATAL, TMP_OUTPUT_FILE);
	    sendTestMessages();
	    Trace.closeDefaultOutput();

	    // Compare contents of log file to array of expected results strings
	    compareOutputs(TMP_OUTPUT_FILE, expectedFatalResults);
	}
	catch (IOException e) {
	    fail("testDefaultWithLevel: exception " + e + " thrown");
	}
    }

    public void testDebugFileOutput() {
	_testFileOutput(Trace.DEBUG, expectedDebugResults);
    }

    public void testFatalFileOutput() {
	_testFileOutput(Trace.FATAL, expectedFatalResults);
    }

    // Also tests "inheritance" because we set output for "Module" but print to
    // "Module", "Module.method()", "Module.method(arg)". We don't have to test
    // here for unassigned modules because that is tested in
    // <code>testStdoutDefault</code>.
    protected void _testFileOutput(int level, String[] expectedResults) {
	try {
	    // Set output to temp file.
	    Trace.setOutput("Module", level, TMP_OUTPUT_FILE);

	    sendTestMessages();

	    // Close output so temp file is flushed and closed.
	    Trace.closeOutput("Module");

	    // Compare contents of log file to array of expected results strings
	    compareOutputs(TMP_OUTPUT_FILE, expectedResults);
	}
	catch (Exception e) {
	    fail("exception " + e + " thrown");
	}
    }

    // Test call to setOutput with System.err as argument.
    public void testStreamOutput() {
	try {
	    // Reset system err to known file
	    PrintStream origSystemErr = System.err;
	    System.setErr(new PrintStream(new FileOutputStream(TMP_OUTPUT_FILE)));

	    // Set output to stderr.
	    Trace.setOutput("Module", Trace.DEBUG, System.err);

	    sendTestMessages();

	    // Restore system err
	    System.setErr(origSystemErr);

	    // Close output so temp file is flushed and closed.
	    Trace.closeOutput("Module");

	    // Compare contents of log file to array of expected results strings
	    compareOutputs(TMP_OUTPUT_FILE, expectedDebugResults);
	}
	catch (Exception e) {
	    fail("exception " + e + " thrown");
	}
    }

    public void testSocketOutput() {
	try {
	    final ServerSocket serverSocket = new ServerSocket(0); // Any free port
	    int port = serverSocket.getLocalPort();

	    // Create separate thread to act as server and listen for input
	    new Thread(new Runnable() {
		public void run() {
		    try {
			// Open socket
			Socket listener = serverSocket.accept();
			byte[] buf = new byte[1024];
			StringBuffer lineBuffer = new StringBuffer();

			// Read one line
			int numBytesRead;
			while (true) {
			    numBytesRead = listener.getInputStream().read(buf);
			    lineBuffer.append(new String(buf, 0, numBytesRead));
			    if (buf[numBytesRead - 1] == (byte)'\n')
				break;
			}

			// Close socket
			listener.close();
			serverSocket.close();

			// Compare expected string to what we received
			String line = lineBuffer.toString();
			int endOfDate = line.indexOf(']');
			assertEquals("] [DEBUG] Module: socket message\n",
				     line.substring(endOfDate));
		    }
		    catch (Exception e) {
			fail("exception " + e + " thrown");
		    }
		}
	    }).start();

	    // Send string to server
	    Trace.setOutput("Module", Trace.DEBUG, InetAddress.getLocalHost(),
			    port);
	    Trace.println("Module", Trace.DEBUG, "socket message");
	}
	catch (Exception e) {
	    fail("exception " + e + " thrown");
	}
    }

    // Send messages common to many tests.
    protected void sendTestMessages() {
	Trace.println("Module", Trace.DEBUG, "module debug");
	Trace.println("Module", Trace.FATAL, "module fatal");
	Trace.println("Module.method()", Trace.DEBUG,
		      "module with method name");
	Trace.println("Module.method(arg)", Trace.DEBUG,
		      "module with method name and arg");
    }

    // Compare output file with array of expected result strings.
    protected void compareOutputs(String outputFileName, String[] expectedResults)
	throws FileNotFoundException, IOException
    {
	BufferedReader in =
	    new BufferedReader(new FileReader(outputFileName));
	String input = null;
	for (int i = 0; i < expectedResults.length; ++i) {
	    input = in.readLine();
	    assertNotNull("unexpected EOF in module output; expected to see <"
			  + expectedResults[i] + ">", input);

	    int endOfDate = input.indexOf(']');
	    assertTrue("compareOutputs: malformed output string (no brackets): '"
		   + input + "'", endOfDate != -1);
	    assertEquals(expectedResults[i], input.substring(endOfDate));
	}

	// Look for extra, unexpected output.
	input = in.readLine();
	assertNull("trace output has too much output; saw <" + input + ">",
		   input);
    }

    public void testFileNameGeneration() {
	try {
	    Trace.setOutput("Module", Trace.DEBUG, TMP_ROOT);
	    Trace.setOutput("Module.method()", Trace.FATAL, TMP_ROOT);

	    assertTrue(generatedTmpOutput.exists());
	    assertTrue(generatedMethodTmpOutput.exists());

	    Trace.closeOutput("Module");
	    Trace.closeOutput("Module.method()");

	    assertTrue(generatedTmpOutput.exists());
	    assertTrue(generatedMethodTmpOutput.exists());
	}
	catch (IOException e) {
	    fail("testFileNameGeneration threw exception " + e);
	}
    }

    // Make sure that writing to a file, then closing it, then using it again
    // appends to the file.
    public void testAppend() {
	try {
	    Trace.setOutput("Module", Trace.DEBUG, TMP_OUTPUT_FILE);
	    Trace.println("Module", Trace.DEBUG, "double the fun");
	    sendTestMessages();
	    Trace.closeOutput("Module");

	    Trace.setOutput("Module", Trace.DEBUG, TMP_OUTPUT_FILE);
	    sendTestMessages();
	    Trace.closeOutput("Module");

	    // Set up expected results: double the pleasure, double the fun
	    String[] doubleExpected =
		new String[expectedDebugResults.length * 2 + 1];
	    doubleExpected[0] = "] [DEBUG] Module: double the fun";
	    System.arraycopy(expectedDebugResults, 0,
			     doubleExpected, 1,
			     expectedDebugResults.length);
	    System.arraycopy(expectedDebugResults, 0,
			     doubleExpected, expectedDebugResults.length,
			     expectedDebugResults.length);

	    for (int i = 0; i < expectedDebugResults.length; ++i)
		doubleExpected[i + 1] =
		    doubleExpected[i + expectedDebugResults.length + 1] =
		    expectedDebugResults[i];

	    compareOutputs(TMP_OUTPUT_FILE, doubleExpected);
	}
	catch (IOException e) {
	    fail("testAppend threw exception " + e);
	}
    }

    // This test deleted by jimm because (a) the default date format was changed
    // by bela and (b) it was a rather simplistic, unnecessary test anyway.

    //  public void testDateFormatting() {
    //      Date d = new Date();
    //      String nullFormat = Format.formatTimestamp(d, null);
    //      String iso = Format.formatTimestamp(d, "yyyy-MM-dd'T'hh:mm:ss,S");
    //      assertEquals(nullFormat, iso);

    //  //      System.out.println(Format.formatTimestamp(d, "MM/dd/yy"));
    //  }

    // Open two modules on the same file. Write output. Close one. Make sure
    // output is what we expect. Write some more via the second module. Check
    // the output again.
    public void testOpenTwoCloseOne() {
	try {
	    // Send output from two different modules to the same file.
	    Trace.setOutput("Module", Trace.DEBUG, TMP_OUTPUT_FILE);
	    Trace.setOutput("AnotherModule", Trace.DEBUG, TMP_OUTPUT_FILE);

	    sendTestMessages();

	    Trace.closeOutput("Module");

	    // Compare contents of log file to array of expected results strings
	    compareOutputs(TMP_OUTPUT_FILE, expectedDebugResults);

	    // What happens when we continue writing via AnotherModule?
	    Trace.println("AnotherModule", Trace.DEBUG, "more output");

	    Trace.closeOutput("AnotherModule");

	    // Set up expected results.
	    String[] expected = new String[expectedDebugResults.length + 1];
	    System.arraycopy(expectedDebugResults, 0, expected, 0,
			     expectedDebugResults.length);
	    expected[expectedDebugResults.length] =
		"] [DEBUG] AnotherModule: more output";

	    // Compare contents of log file to array of expected results strings
	    compareOutputs(TMP_OUTPUT_FILE, expected);
	}
	catch (IOException e) {
	    fail("testOpenTwoCloseOne threw exception " + e);
	}
    }



    
    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }



}
