package org.jgroups.demos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.JChannel;
import org.jgroups.blocks.executor.ExecutionCompletionService;
import org.jgroups.blocks.executor.ExecutionRunner;
import org.jgroups.blocks.executor.ExecutionService;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

public class ExecutionServiceDemo {
    protected String props;
    protected JChannel ch;
    protected ExecutionService execution_service;
    protected String name;
    protected ExecutionRunner runner;
    protected int size;
    protected boolean printValues;
    protected Random random;
    
    protected ExecutorService executor;
    protected Queue<Future<?>> queue;

    public ExecutionServiceDemo(String props, String name, int size) {
        this.props=props;
        this.name=name;
        queue=new ArrayDeque<Future<?>>();
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "Consumer-" + 
                    poolNumber.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
            AtomicInteger poolNumber = new AtomicInteger();
        });
        this.size=size;
    }
    
    public static void main(String[] args) throws Exception {
        String props=null;
        String name=null;
        String size="1000";
        

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }


            help();
            return;
        }

        ExecutionServiceDemo demo=new ExecutionServiceDemo(props, name, 
            Integer.valueOf(size));
        demo.start();
    }
    
    public void start() throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        execution_service=new ExecutionService(ch);
        runner=new ExecutionRunner(ch);
        ch.connect("executing-cluster");
        JmxConfigurator.registerChannel(ch, Util.getMBeanServer(), 
            "execution-service", ch.getClusterName(), true);
        
        // Start a consumer
        queue.add(executor.submit(runner));
        random = new Random();
        printValues = false;

        try {
            loop();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            Util.close(ch);
        }
    }
    
    public static class SortingByteCallable implements Callable<byte[]>, Streamable {
        public SortingByteCallable() {
        }
        public SortingByteCallable(byte[] bytes, int offset, int size) {
            this.bytes=bytes;
            this.offset=offset;
            this.size=size;
        }
        
        @Override
        public byte[] call() throws Exception {
            Arrays.sort(bytes);
            return bytes;
        }
        
        protected byte[] bytes;
        protected int offset = 0;
        protected int size = 0;

        // We copy over as a single array with no offset
        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(size);
            out.write(bytes, offset, size);
        }

        @Override
        public void readFrom(DataInput in) throws IOException,
                IllegalAccessException, InstantiationException {
            bytes = new byte[in.readInt()];
            in.readFully(bytes, 0, bytes.length);
        }
    }
    
    /**
     * Sorts 2 byte arrys into a larger byte array
     * 
     * @author wburns
     */
    public static class SortingTwoByteCallable implements Callable<byte[]>, Streamable {
        protected byte[] bytes1;
        protected byte[] bytes2;
        
        public SortingTwoByteCallable() {
            
        }
        public SortingTwoByteCallable(byte[] ints1, byte[] ints2) {
            this.bytes1=ints1;
            this.bytes2=ints2;
        }
        
        @Override
        public byte[] call() throws Exception {
            byte[] results = new byte[bytes1.length + bytes2.length];
            int i = 0;
            int j = 0;
            while (i < bytes1.length && j < bytes2.length) {
                if (bytes1[i] < bytes2[j]) {
                    results[i + j] = bytes1[i++];
                }
                else {
                    results[i + j] = bytes2[j++];
                }
            }
            if (i < bytes1.length) {
                System.arraycopy(bytes1, i, results, i + j, bytes1.length - i);
            }
            else if (j < bytes2.length) {
                System.arraycopy(bytes2, j, results, i + j, bytes2.length - j);
            }
            return results;
        }
        
        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(bytes1.length);
            out.write(bytes1);
            out.writeInt(bytes2.length);
            out.write(bytes2);
        }

        @Override
        public void readFrom(DataInput in) throws IOException,
                IllegalAccessException, InstantiationException {
            int size = in.readInt();
            bytes1 = new byte[size];
            
            in.readFully(bytes1);
            
            size = in.readInt();
            bytes2 = new byte[size];
            
            in.readFully(bytes2);
        }
    }
    
    protected void loop() throws Exception {
        while(ch.isConnected()) {
            String line=Util.readStringFromStdin(": ");
            if(line.startsWith("quit") || line.startsWith("exit"))
                break;

            if(line.startsWith("submit")) {
                int randomNumbers = Integer.parseInt(line.substring("submit".length()).trim());
                // Parse numbers and break into parts
                byte[] numbers = new byte[randomNumbers];
                
                for (int i = 0; i < randomNumbers; ++i) {
                    numbers[i] = (byte)random.nextInt(256);
                }
                
                ExecutionCompletionService<byte[]> completion = 
                    new ExecutionCompletionService<byte[]>(execution_service);
                
                long beginDistributed = System.nanoTime();
                int chunks = numbers.length / size;
                for (int i = 0; i < chunks; ++i) {
                    completion.submit(new SortingByteCallable(numbers, size * i, size));
                }

                int futureNumber = chunks;
                int leftOver = numbers.length % size;
                if (leftOver != 0) {
                    completion.submit(new SortingByteCallable(numbers, numbers.length - leftOver, leftOver));
                    futureNumber++;
                }
                
                Future<byte[]> finalValue;
                if (futureNumber > 1) {
                    Future<byte[]> result = null;
                    while (true) {
                        result = completion.take();
                        if (--futureNumber >= 1) {
                            Future<byte[]> result2 = completion.take();
                            completion.submit(new SortingTwoByteCallable(result.get(), result2.get()));
                        }
                        else {
                            break;
                        }
                    }
                    
                    finalValue = result;
                }
                else {
                    finalValue = completion.take();
                }
                
                byte[] results = finalValue.get();
                
                long totalDistributed = System.nanoTime() - beginDistributed;
                if (printValues) {
                    System.out.println("Original Numbers: "
                            + Arrays.toString(numbers));
                    System.out.println("Sorted values: " + Arrays.toString(
                        results));
                }
                System.out.println("Distributed Sort Took: " + Util.printTime(totalDistributed, TimeUnit.NANOSECONDS));
                
                long beginLocal = System.nanoTime();
                Arrays.sort(numbers);
                System.out.println("      Local Sort Took: " + Util.printTime((System.nanoTime() - beginLocal), TimeUnit.NANOSECONDS));
            }
            else  if(line.startsWith("consumer")) {
                // Parse stop start and add or remove
                if (line.contains("start")) {
                    queue.add(executor.submit(runner));
                    System.out.println("Started Consumer - running " + queue.size() + " consumers");
                }
                else if (line.contains("stop")) {
                    queue.remove().cancel(true);
                    System.out.println("Stopped Consumer - running " + queue.size() + " consumers");
                }
                else {
                    System.out.println("Consumers Running Locally: " + queue.size());
                }
            }
            else if(line.startsWith("size")) {
                String thresholdSize = line.substring("size".length()).trim();
                if (thresholdSize.length() > 0) {
                    int size = Integer.parseInt(thresholdSize);
                    
                    this.size = size;
                    System.out.println("Changed sort threshold size to " + size);
                }
                else {
                    System.out.println("Threshold Size: " + size);
                }
            }
            else if(line.startsWith("print")) {
                printValues = !printValues;
                System.out.println("Print Arrays: " + printValues);
            }
            else if(line.startsWith("view"))
                System.out.println("View: " + ch.getView());
            else if(line.startsWith("help"))
                help();
        }
    }


    protected static void help() {
        System.out.println("\nExecutionServiceDemo [-props properties] [-name name]\n" +
                             "Default Values:\n\n" +
                             "One Consumer\n" +
                             "Threshold size: 1000\n" +
                             "Print disabled\n\n" +
                             "Valid commands:\n\n" +
                             "submit (amount of numbers to generate)\n" +
                             "consumer (start) | (stop)\n" +
                             "size (value)\n" +
                             "print");
        System.out.println("\nExample:\nsubmit 2000000\nconsumer start\nconsumer stop\nsize 1000000\nprint");
    }
}
