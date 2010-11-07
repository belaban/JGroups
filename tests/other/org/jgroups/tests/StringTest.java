package org.jgroups.tests;

/**
 * @author Bela Ban
 * @version $Id: StringTest.java,v 1.2 2008/01/22 10:44:28 belaban Exp $
 */
public class StringTest {
    final int NUM=1000000;
    long start, stop;

    public static void main(String[] args) {
        new StringTest().start();
    }

    private void start() {
        rawStringsWithObjects();
        rawStringsWithLiterals();
        StringBuilder();
    }


    private void rawStringsWithObjects() {
        String result=null;
        String a="a", b="b", c="c", d="d";
        long time=System.currentTimeMillis();
        start=System.currentTimeMillis();
        for(int i=0; i < NUM; i++) {
            result=a + b + c + d + "ecdsfh" + time;
        }
        stop=System.currentTimeMillis();
        System.out.println("total time for rawStringsWithObjects(): " + (stop-start));
        System.out.println("result=" + result);
    }

    private void rawStringsWithLiterals() {
        String result=null;
        long time=System.currentTimeMillis();
        start=System.currentTimeMillis();
        for(int i=0; i < NUM; i++) {
            result="a" + "b" + "c" + "d" + "ecdsfh" + time; // needs runtime resolution
            // result="a" + "b" + "c" + "d" + "ecdsfh" + 322463;  // is concatenated at *compile time*
        }
        stop=System.currentTimeMillis();
        System.out.println("total time for rawStringsWithLiterals(): " + (stop-start));
        System.out.println("result=" + result);
    }

    private void StringBuilder() {
        String result=null;
        StringBuilder sb;
        long time=System.currentTimeMillis();
        start=System.currentTimeMillis();
        for(int i=0; i < NUM; i++) {
            sb=new StringBuilder("a");
            sb.append("b").append("c").append("d").append("ecdsfh").append(time);
            result=sb.toString();
        }
        stop=System.currentTimeMillis();
        System.out.println("total time for StringBuilder(): " + (stop-start));
        System.out.println("result=" + result);
    }
}
