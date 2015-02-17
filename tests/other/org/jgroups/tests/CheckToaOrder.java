package org.jgroups.tests;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This check the order of the messages, assuming that the message IDs are printed in a file
 * 
 * Note: this is used for debugging
 * Note2: this needs to be clean :)
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class CheckToaOrder {

    public static void main(String[] args) throws InterruptedException {

        if (args.length == 0) {
            System.err.println("usage: [-threads value] <files...>");
            System.exit(1);
        }

        int numberOfThreads = 2;
        int startIdx = 0;

        if (args[0].equals("-threads")) {
            startIdx = 2;
            try {
                numberOfThreads = Integer.parseInt(args[1]);
            } catch (Exception e) {
                System.err.println("Error parsing number of threads: " + e.getLocalizedMessage());
                numberOfThreads = 2;
            } finally {
                if (numberOfThreads < 1) {
                    numberOfThreads = 2;
                }
            }
        }

        System.out.println("--------------------------------------------------------------------");
        System.out.println("----------------------- CHECK TOA ORDER ----------------------------");
        System.out.println("--------------------------------------------------------------------");

        System.out.println("analyze " + printArgs(args) + " using " + numberOfThreads + " threads");
        
        ComparingFiles[] threads = new ComparingFiles[numberOfThreads];
        
        List<Pair> allCombinations = new LinkedList<>();
        
        for (int x = startIdx; x < args.length; ++x) {
            for (int y = x + 1; y < args.length; ++y) {
                if (x == y) {
                    continue;
                }
                allCombinations.add(new Pair(x, y));
            }
        }
        
        System.out.println("Collection for the threads is " + allCombinations);
        
        final Iterator<Pair> iterator = allCombinations.iterator();
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new ComparingFiles(iterator,args,"Comparator-" + i);
            threads[i].start();
        }

        for (ComparingFiles comparingFiles : threads) {
            comparingFiles.join();
        }

        System.out.println("=========== FINISHED! ==============");
    }

    private static String printArgs(String[] args) {
        StringBuilder sb = new StringBuilder(args[0]);

        for (int i = 1;  i < args.length; ++i) {
            sb.append(",").append(args[i]);
        }
        return sb.toString();
    }

    public static void compareFiles(String filePath1, String filePath2, List<String> messageDeliverOrder) {
        System.out.println("Comparing " + filePath1 + " and " + filePath2 + " by thread " + Thread.currentThread().getName());
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath1));

            String message;

            while ((message = bufferedReader.readLine()) != null) {
                messageDeliverOrder.add(message);
            }

            bufferedReader.close();
            bufferedReader = new BufferedReader(new FileReader(filePath2));
            int msgExpectedIdx = 0;

            while ((message = bufferedReader.readLine()) != null) {
                if (msgExpectedIdx >= messageDeliverOrder.size()) {
                    messageDeliverOrder.add(message);
                    ++msgExpectedIdx;
                    continue;
                }
                String otherMessage = messageDeliverOrder.get(msgExpectedIdx);
                if (otherMessage.equals(message)) {
                    msgExpectedIdx++;
                    continue;
                }

                int realMsgIdx = messageDeliverOrder.indexOf(message);

                if (realMsgIdx == -1) {
                    continue;
                } else if (realMsgIdx < msgExpectedIdx) {
                    System.err.println("[" + Thread.currentThread().getName() + "] Message deliver out of order: " + message);
                }
                msgExpectedIdx = realMsgIdx + 1;
            }

            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } catch (IOException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } finally {
            System.out.println("Finished comparing this files");
        }
    }

    /*public static void processFile(String filePath, List<String> messageDeliverOrder) {
        System.out.println("Processing " + filePath + " by thread " + Thread.currentThread().getName());
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));

            int msgExpectedIdx = 0;
            String message;

            while ((message = bufferedReader.readLine()) != null) {
                if (msgExpectedIdx >= messageDeliverOrder.size()) {
                    messageDeliverOrder.add(message);
                    ++msgExpectedIdx;
                    continue;
                }
                String otherMessage = messageDeliverOrder.get(msgExpectedIdx);
                if (otherMessage.equals(message)) {
                    msgExpectedIdx++;
                    continue;
                }

                int realMsgIdx = messageDeliverOrder.indexOf(message);

                if (realMsgIdx == -1) {
                    messageDeliverOrder.add(msgExpectedIdx, message);
                    msgExpectedIdx++;
                    continue;
                } else if (realMsgIdx < msgExpectedIdx) {
                    System.err.println("[" + Thread.currentThread().getName() + "] Message deliver out of order: " +
                            message + ". Real Idx:" + realMsgIdx + ", Expected Idx:" + msgExpectedIdx);
                }
                msgExpectedIdx = realMsgIdx + 1;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } catch (IOException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } finally {
            System.out.println(Thread.currentThread().getName() + " finished processing the file. Getting another one");
        }
    }

    public static void joinLists(List<String> toJoin, ArrayList<String> messageDeliverOrder) {
        int joinSize = toJoin.size();
        messageDeliverOrder.ensureCapacity(joinSize);
        System.out.println("Joining lists. Sizes: " + joinSize + " and " + messageDeliverOrder.size());

        int msgExpectedIdx = 0;
        for (String message : toJoin) {
            if (msgExpectedIdx >= messageDeliverOrder.size()) {
                messageDeliverOrder.add(message);
                ++msgExpectedIdx;
                continue;
            }
            String otherMessage = messageDeliverOrder.get(msgExpectedIdx);
            if (otherMessage.equals(message)) {
                msgExpectedIdx++;
                continue;
            }

            int realMsgIdx = messageDeliverOrder.indexOf(message);

            if (realMsgIdx == -1) {
                messageDeliverOrder.add(msgExpectedIdx, message);
                msgExpectedIdx++;
                continue;
            } else if (realMsgIdx < msgExpectedIdx) {
                System.err.println("[Joining] Message deliver out of order: " + message + ". Real Idx:" + realMsgIdx +
                        ", Expected Idx:" + msgExpectedIdx);
                continue;
            }
            msgExpectedIdx = realMsgIdx + 1;
        }

    }

    private static class ProcessingFile extends Thread {
        private AtomicInteger argumentIdx;
        private ArrayList<String> messageDeliverOrder = new ArrayList<String>();
        private String[] args;

        private ProcessingFile(final AtomicInteger argumentIdx, final String[] args, String threadName) {
            super(threadName);
            this.argumentIdx = argumentIdx;
            this.args = args;
        }

        @Override
        public void run() {
            while (true) {
                int idx = argumentIdx.getAndIncrement();
                if (idx >= args.length) {
                    break;
                }
                processFile(args[idx], messageDeliverOrder);
            }
            System.out.println(getName() + " finished!");
        }
    }*/
    
    private static class Pair {       
        private final int x, y;

        private Pair(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "y=" + y +
                    ", x=" + x +
                    '}';
        }
    }
    
    private static class ComparingFiles extends Thread {
        private final Iterator<Pair> filesToCompare;
        private ArrayList<String> messageDeliverOrder = new ArrayList<>();
        private String[] args;

        private ComparingFiles(final Iterator<Pair> filesToCompare, final String[] args, String threadName) {
            super(threadName);
            this.filesToCompare = filesToCompare;
            this.args = args;
        }

        @Override
        public void run() {
            while (true) {
                Pair p;
                synchronized (filesToCompare) {
                    if (!filesToCompare.hasNext()) {
                        break;
                    }
                    p = filesToCompare.next();
                }
                compareFiles(args[p.getX()], args[p.getY()], messageDeliverOrder);
                int size = messageDeliverOrder.size();
                messageDeliverOrder.clear();
                messageDeliverOrder.ensureCapacity(size);
            }            
            
            System.out.println(getName() + " finished!");
        }
    }
}
