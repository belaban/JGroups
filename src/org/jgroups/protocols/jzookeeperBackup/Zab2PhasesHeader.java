package org.jgroups.protocols.jzookeeperBackup;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.protocols.jzookeeper.MessageId;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;


    public class Zab2PhasesHeader extends Header implements Comparable<Zab2PhasesHeader> {
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
//         public static final byte TEMPSENT = 14;
//         public static final byte TEMPREC = 15;
//         public static final byte RESPONCETEMP = 16;


         private byte        type=0;
         private long        zxid=0;
         private MessageId   messageId=null;
         private Address ackedFollower = null;

        public Zab2PhasesHeader() {
        }

        public Zab2PhasesHeader(byte type) {
            this.type=type;
        }
        public Zab2PhasesHeader(byte type, MessageId id) {
            this.type=type;
            this.messageId=id;
        }

        public Zab2PhasesHeader(byte type, long zxid) {
            this(type);
            this.zxid=zxid;
        }
        public Zab2PhasesHeader(byte type, long zxid, MessageId messageId) {
            this(type);
            this.zxid=zxid;
            this.messageId=messageId;
        }
        
        public Zab2PhasesHeader(byte type, long zxid, MessageId messageId, Address ackedFollower) {
            this(type);
            this.zxid=zxid;
            this.messageId=messageId;
            this.ackedFollower=ackedFollower;
        }
    
        public byte getType() {
			return type;
		}

		public void setType(byte type) {
			this.type = type;
		}
		
		

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((messageId == null) ? 0 : messageId.hashCode());
			result = prime * result + (int) (zxid ^ (zxid >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Zab2PhasesHeader other = (Zab2PhasesHeader) obj;
			if (messageId == null) {
				if (other.messageId != null)
					return false;
			} else if (!messageId.equals(other.messageId))
				return false;
			if (zxid != other.zxid)
				return false;
			return true;
		}

		public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(zxid >= 0)
                sb.append(" zxid=" + zxid);
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
//            case TEMPSENT:			 return "TEMPSENT";
//            case TEMPREC:			 return "TEMPREC";
//            case RESPONCETEMP:			 return "RESPONCETEMP";

            default:             return "n/a";
        }
        }
        
        public long getZxid() {
            return zxid;
        }
        
        public MessageId getMessageId(){
        	return messageId;
        }
        
        public Address getAckedFollower(){
        	return ackedFollower;
        }
        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(zxid,out);
            Util.writeStreamable(messageId, out);
            Util.writeAddress(ackedFollower, out);
            //messageId.writeTo(out);
            //out.writeBoolean(flush_ack);
        }
        
        @Override
        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            zxid=Bits.readLong(in);
            //messageId = new MessageId();
            //messageId.readFrom(in);
            messageId = (MessageId) Util.readStreamable(MessageId.class, in); 
            ackedFollower = Util.readAddress(in);
           // flush_ack=in.readBoolean();
        }
        
        @Override
        public int size() {
        	//(messageInfo != null ? messageInfo.size() : 0)
            return Global.BYTE_SIZE + Bits.size(zxid) + (messageId != null ? messageId.size(): 0) + Global.BYTE_SIZE +
            		Util.size(ackedFollower); 
         }

        
		@Override
		public int compareTo(Zab2PhasesHeader nextHeader) {
			long thisZxid = this.zxid;
            long nextZxid = nextHeader.getZxid();

            if (thisZxid==nextZxid)
                return 0;
            else if (thisZxid > nextZxid)
                return 1;
            else
                return -1;
		}

    }