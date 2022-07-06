package org.jgroups.tests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.BiConsumer;

import org.jgroups.Message;
import org.jgroups.util.MessageBatch;

/**
 * While investigating network errors, we would like to look for timestamp and JGroups data.
 * But when needed, we need to add more fields like source and destination ports.
 *
 * Example: tshark -q -i lo0 -Tfields -e frame.time_epoch -e udp.srcport -e udp.dstport -e data udp and port 9090
 *
 * Run: ParseMessages -instance org.jgroups.tests.ParseMessagesFields -Dfields="frame.time_epoch,udp.srcport,udp.dstport"
 */
public class ParseMessagesFields extends ParseMessages {

   private static final DateTimeFormatter TIME_EPOCH_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss,SSS");
   private static final ZoneId ZONE_ID=ZoneId.systemDefault();

   private static String[] fields;
   static {
      String fieldsArgument = System.getProperty("fields");
      if (fieldsArgument != null) {
         fields =  fieldsArgument.split(",");
      }
   }

   @Override
   protected InputStream createInputStream(InputStream in) {
      return new BinaryToAsciiWithFieldsInputStream(in);
   }

   @Override
   public void parse(InputStream in, BiConsumer<Short,Message> msg_consumer,
                     BiConsumer<Short,MessageBatch> batch_consumer, boolean tcp) {
      parseWithFields(in, msg_consumer, batch_consumer, tcp);
   }

   public static class BinaryToAsciiWithFieldsInputStream extends ParseMessages.BinaryToAsciiInputStream {

      public BinaryToAsciiWithFieldsInputStream(InputStream in) {
         super(in);
      }

      public String readIntegerAsStr() throws IOException {
         String str = "";
         while (true) {
            input[0] = (byte)in.read();
            if (input[0] == '\t') {
               break;
            }
            input[1] = (byte)in.read();
            // tshark \t separator
            if (input[1] == '\t') {
               str += new String(new byte[]{input[0]});
               break;
            } else {
               str += new String(input);
            }
         }
         return str;
      }

      public String readEpochTime() throws IOException {
         String timestamp="";
         while (timestamp.length() != 20) {
            input[0] = (byte)in.read();
            if (input[0] == '\n' || input[0] == '\r') {
               continue;
            }
            input[1] = (byte)in.read();
            // end of file
            if (input[0] == -1 || input[1] == -1) {
               throw new EOFException();
            }
            timestamp+=new String(input);
         }
         // tshark \t separator
         readPlainByte();

         String[] time = timestamp.split("\\.");
         long epochMilli = Long.valueOf(time[0]) * 1000;
         long nanos = Long.valueOf(time[1]);
         ZonedDateTime zonedDateTime = Instant.ofEpochMilli(epochMilli).plusNanos(nanos).atZone(ZONE_ID);
         String epochTime = TIME_EPOCH_DATE_TIME_FORMATTER.format(zonedDateTime);
         return epochTime;
      }

      public byte readPlainByte() throws IOException {
         return (byte)in.read();
      }
   }

   public void parseWithFields(InputStream input, BiConsumer<Short, Message> msg_consumer,
                                         BiConsumer<Short, MessageBatch> batch_consumer, boolean tcp) {
      try {
         for(;;) {
            // parse fields
            String[] fieldValues = new String[fields.length];
            BinaryToAsciiWithFieldsInputStream cast = ((BinaryToAsciiWithFieldsInputStream) input);
            for (int i = 0; i < fields.length; i++) {
               String field = fields[i];
               if ("frame.time_epoch".equals(field)) {
                  fieldValues[i] = cast.readEpochTime();
               } else if (field.contains(".srcport")) {
                  fieldValues[i] = cast.readIntegerAsStr();
               } else if (field.contains(".dstport")) {
                  fieldValues[i] = cast.readIntegerAsStr();
               } else {
                  throw new RuntimeException(String.format("%s not implemented", field));
               }
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // after the \t separator, the data field could be empty
            for(;;) {
               byte b = cast.readPlainByte();
               if (b == '\n' || b == '\r' || b == -1) {
                  break;
               }
               outputStream.write(b);
            }
            if (outputStream.size() != 0) {
               for (String fieldValue : fieldValues) {
                  System.out.print(fieldValue + " ");
               }
               super.parse(new BinaryToAsciiInputStream(new ByteArrayInputStream(outputStream.toByteArray())), msg_consumer, batch_consumer, tcp);
            }
         }
      } catch(EOFException ignored) {
         //ignored.printStackTrace();
      } catch (IOException e) {
         e.printStackTrace();
      }

   }
}
