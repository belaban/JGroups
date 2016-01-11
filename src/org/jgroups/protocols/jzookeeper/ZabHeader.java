package org.jgroups.protocols.jzookeeper;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;


    public class ZabHeader extends Header {
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
         public static final byte SENDMYADDRESS = 14;



         private byte        type=0;
         private long        seqno=0;
         private MessageId   messageId=null;
         private MessageOrderInfo messageOrderInfo = null;


        public ZabHeader() {
        }

        public ZabHeader(byte type) {
            this.type=type;
        }
        
        public ZabHeader(MessageOrderInfo messageOrderInfo) {
			this.messageOrderInfo = messageOrderInfo;
		}

		public ZabHeader(byte type, MessageId id) {
            this.type=type;
            this.messageId=id;
        }
		
		public ZabHeader(byte type, MessageOrderInfo messageOrderInfo) {
            this.type=type;
            this.messageOrderInfo=messageOrderInfo;
        }
		
		public ZabHeader(byte type, MessageOrderInfo messageOrderInfo, MessageId messageId) {
            this.type=type;
            this.messageOrderInfo=messageOrderInfo;
            this.messageId=messageId;

        }
		
		public ZabHeader(byte type, long seqno, MessageOrderInfo messageOrderInfo, MessageId messageId) {
            this.type=type;
            this.seqno=seqno;
            this.messageOrderInfo=messageOrderInfo;
            this.messageId=messageId;

        }

        public ZabHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }
        public ZabHeader(byte type, long seqno, MessageId messageId) {
            this(type);
            this.seqno=seqno;
            this.messageId=messageId;
        }
        
        public ZabHeader(byte type, long seqno, MessageOrderInfo messageOrderInfo) {
            this(type);
            this.seqno=seqno;
            this.messageOrderInfo=messageOrderInfo;
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
            case SENDMYADDRESS:			 return "SENDMYADDRESS";
            default:             return "n/a";
        }
        }
        
        public long getZxid() {
            return seqno;
        }
        
        public MessageId getMessageId(){
        	return messageId;
        }
        
        
        public MessageOrderInfo getMessageOrderInfo() {
			return messageOrderInfo;
		}

		public void setMessageOrderInfo(MessageOrderInfo messageOrderInfo) {
			this.messageOrderInfo = messageOrderInfo;
		}

		@Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            Util.writeStreamable(messageId, out);
            Util.writeStreamable(messageOrderInfo, out);
            //writeMessageOrderInfo(messageOrderInfo, out);
            //messageId.writeTo(out);
            //out.writeBoolean(flush_ack);
        }
        
        @Override
        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            messageId = new MessageId();
            messageId = (MessageId) Util.readStreamable(MessageId.class, in); 
            messageOrderInfo = new MessageOrderInfo();
            messageOrderInfo = (MessageOrderInfo) Util.readStreamable(MessageOrderInfo.class, in); 

            //messageOrderInfo = readMessageOrderInfo(in);
        }
        
        private void writeMessageOrderInfo(MessageOrderInfo info, DataOutput out) throws Exception {
            if (info == null) {
                out.writeShort(-1);
            } else {
                out.writeShort(1);
                info.writeTo(out);
            }
        }

        private MessageOrderInfo readMessageOrderInfo(DataInput in) throws Exception {
            short length = in.readShort();
            if (length < 0) {
                return null;
            } else {
                MessageOrderInfo info = new MessageOrderInfo();
                info.readFrom(in);
                return info;
            }
        }
        
        @Override
        public int size() {
        	//(messageInfo != null ? messageInfo.size() : 0)
            return Global.BYTE_SIZE + Bits.size(seqno) + (messageId != null ? messageId.size(): 0) + (messageOrderInfo != null ? messageOrderInfo.size() : 0); 
         }

    }