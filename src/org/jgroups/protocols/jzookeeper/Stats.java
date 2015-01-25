package org.jgroups.protocols.jzookeeper;

public class Stats {
        private long    start=0;
        private long    end=0; // done when > 0
        private long    num_msgs_received=0;
        private long    seqno=1; // next expected seqno

        public void reset() {
            start=end=num_msgs_received=0;
            seqno=1;
        }

        public void    end() {end=System.currentTimeMillis();}
        //public boolean isDone() {return stop > 0;}

        /**
         * Adds the message and checks whether the messages are received in FIFO order. If we have multiple threads
         * (check_order=false), then this check canot be performed
         * @param seqno
         * @param check_order
         */
        public void addMessage() {
                start=System.currentTimeMillis();
        }

        public String toString() {
            return "end - start= "+ (end - start);
        }
    }