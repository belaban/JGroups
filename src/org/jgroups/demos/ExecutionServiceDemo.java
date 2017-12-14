package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.blocks.executor.ExecutionCompletionService;
import org.jgroups.blocks.executor.ExecutionRunner;
import org.jgroups.blocks.executor.ExecutionService;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
        queue=new ArrayDeque<>();
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
    
    protected static class ByteBufferStreamable implements Streamable {
        
        protected ByteBuffer buffer;
        
        public ByteBufferStreamable() {

        }
        
        protected ByteBufferStreamable(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            int sizeLocal = buffer.limit() - buffer.position();
            out.writeInt(sizeLocal);
            out.write(buffer.array(), buffer.position(), sizeLocal);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            buffer = ByteBuffer.allocate(in.readInt());
            in.readFully(buffer.array());
        }
        
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
    
    public static class SortingByteCallable implements Callable<ByteBufferStreamable>, Streamable {
        public SortingByteCallable() {
        }
        public SortingByteCallable(byte[] bytes, int offset, int size) {
            buffer = ByteBuffer.wrap(bytes, offset, size);
        }
        
        @Override
        public ByteBufferStreamable call() throws Exception {
            Arrays.sort(buffer.array(), buffer.position(), buffer.limit());
            return new ByteBufferStreamable(buffer);
        }
        
        protected ByteBuffer buffer;

        // We copy over as a single array with no offset
        @Override
        public void writeTo(DataOutput out) throws Exception {
            Util.writeStreamable(new ByteBufferStreamable(buffer), out);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            buffer = Util.readStreamable(ByteBufferStreamable::new, in).buffer;
        }
    }
    
    /**
     * Sorts 2 byte arrys into a larger byte array
     * 
     * @author wburns
     */
    public static class SortingTwoByteCallable implements Callable<ByteBufferStreamable>, Streamable {
        protected ByteBuffer bytes1;
        protected ByteBuffer bytes2;
        
        public SortingTwoByteCallable() {
            
        }
        public SortingTwoByteCallable(ByteBufferStreamable bytes1, ByteBufferStreamable bytes2) {
            this.bytes1=bytes1.buffer;
            this.bytes2=bytes2.buffer;
        }
        
        @Override
        public ByteBufferStreamable call() throws Exception {
            ByteBuffer results = ByteBuffer.allocate(bytes1.remaining() + 
                bytes2.remaining());
            int i = bytes1.position();
            int j = bytes2.position();
            byte[] byteArray1 = bytes1.array();
            byte[] byteArray2 = bytes2.array();
            int byte1Max = bytes1.limit();
            int byte2Max = bytes2.limit();
            while (i < byte1Max && j < byte2Max) {
                if (byteArray1[i] < byteArray2[j]) {
                    results.put(byteArray1[i++]);
                }
                else {
                    results.put(byteArray2[j++]);
                }
            }
            if (i < byte1Max) {
                results.put(byteArray1, i, byte1Max - i);
            }
            else if (j < byte2Max) {
                results.put(byteArray2, j, byte2Max - j);
            }
            results.flip();
            return new ByteBufferStreamable(results);
        }
        
        @Override
        public void writeTo(DataOutput out) throws Exception {
            Util.writeStreamable(new ByteBufferStreamable(bytes1), out);
            Util.writeStreamable(new ByteBufferStreamable(bytes2), out);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            bytes1 = Util.readStreamable(ByteBufferStreamable::new, in).buffer;
            bytes2 = Util.readStreamable(ByteBufferStreamable::new, in).buffer;
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
                
                if (printValues)
                    System.out.println("Original Numbers: " + 
                        Arrays.toString(numbers));
                
                ExecutionCompletionService<ByteBufferStreamable> completion = 
                    new ExecutionCompletionService<>(execution_service);
                
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
                
                Future<ByteBufferStreamable> finalValue;
                if (futureNumber > 1) {
                    Future<ByteBufferStreamable> result = null;
                    while (true) {
                        result = completion.take();
                        if (--futureNumber >= 1) {
                            Future<ByteBufferStreamable> result2 = completion.take();
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
                
                ByteBufferStreamable results = finalValue.get();
                
                long totalDistributed = System.nanoTime() - beginDistributed;
                if (printValues) {
                    System.out.println("Sorted values: " + Arrays.toString(
                        results.buffer.array()));
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
                    int sizeLocal = Integer.parseInt(thresholdSize);
                    
                    this.size = sizeLocal;
                    System.out.println("Changed sort threshold size to " + sizeLocal);
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
