package org.jgroups.protocols.jzookeeper;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;


    public class ZABHeader extends Header {
         public static final byte REQUEST       = 1;
         public static final byte FORWARD       = 2;
         public static final byte PROPOSAL      = 3;
         public static final byte ACK           = 4;
         public static final byte COMMIT        = 5;
         public static final byte RESPONSE      = 6;
         public static final byte DELIVER       = 7;
         public static final byte START_SENDING = 8;
         public static final byte COMMITOUTSTANDINGREQUESTS = 9;
         public static final byte RESET = 10;
         public static final byte STATS = 11;
         public static final byte COUNTMESSAGE = 12;
         public static final byte STARTREALTEST = 13;




         

         private byte        type=0;
         private long        seqno=0;
         private MessageId   messageId=null;

        public ZABHeader() {
        }

        public ZABHeader(byte type) {
            this.type=type;
        }
        public ZABHeader(byte type, MessageId id) {
            this.type=type;
            this.messageId=id;
        }

        public ZABHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }
        public ZABHeader(byte type, long seqno, MessageId messageId) {
            this(type);
            this.seqno=seqno;
            this.messageId=messageId;
        }
    
        public byte getType() {
			return type;
		}

		public void setType(byte type) {
			this.type = type;
		}

		public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(messageId!=null)
            	sb.append(", message_id=" + messageId);
            return sb.toString();
        }

        protected final String printType() {
        	
        	switch(type) {
            case REQUEST:        return "REQUEST";
            case FORWARD:        return "FORWARD";
            case PROPOSAL:       return "PROPOSAL";
            case ACK:            return "ACK";
            case COMMIT:         return "COMMIT";
            case RESPONSE:       return "RESPONSE";
            case DELIVER:        return "DELIVER";
            case START_SENDING:  return "START_SENDING";
            case COMMITOUTSTANDINGREQUESTS:  return "COMMITOUTSTANDINGREQUESTS";
            case RESET:          return "RESET";
            case STATS:			 return "STATS";
            case COUNTMESSAGE:			 return "COUNTMESSAGE";
            case STARTREALTEST:			 return "STARTREALTEST";
            default:             return "n/a";
        }
           
        }
        
        public long getZxid() {
            return seqno;
        }
        
        public MessageId getMessageId(){
        	return messageId;
        }
        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            Util.writeStreamable(messageId, out);
            //messageId.writeTo(out);
            //out.writeBoolean(flush_ack);
        }
        
        @Override
        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            //messageId = new MessageId();
            //messageId.readFrom(in);
            messageId = (MessageId) Util.readStreamable(MessageId.class, in); 
           // flush_ack=in.readBoolean();
        }
        
        @Override
        public int size() {
        	//(messageInfo != null ? messageInfo.size() : 0)
            return Global.BYTE_SIZE + Bits.size(seqno) + (messageId != null ? messageId.serializedSize(): 0) + Global.BYTE_SIZE; 
         }

    }