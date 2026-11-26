package org.jgroups.tests;

/**
 * Tests poly- and megamorphic calls wrt inlining using JITWatch (https://www.youtube.com/watch?v=p7ipmAa9_9E)
 * @author Chris Newland
 * @since  5.4.6
 */
public class PolymorphismTest {
    public interface Coin {void deposit();}
    public static int moneyBox=0;
    public static int calls=0;
    static final int max_impls=2;

    public class Nickel implements Coin {
        @Override public void deposit() {
            moneyBox+=5;
        }
    }

    public class Dime implements Coin {
        @Override public void deposit() {
            moneyBox+=10;
        }
    }

    public class Quarter implements Coin {
        @Override public void deposit() {
            moneyBox+=25;
        }
    }

    public void start() {
        Coin nickel=new Nickel();
        Coin dime=new Dime();
        Coin quarter=new Quarter();
        Coin coin=null;
        for(int i=0; i < 100_000; i++) {
            int idx=i % max_impls;
            switch(idx) {
                case 0:
                    coin=nickel;
                    break;
                case 1:
                    coin=dime;
                    break;
                case 2:
                    coin=quarter;
                    break;
            }
            coin.deposit();
            calls++;
        }
        System.out.printf("moneyBox = %s, calls: %,d\n", moneyBox, calls);
    }

    public static void main(String[] args) {
        new PolymorphismTest().start();
    }

}
