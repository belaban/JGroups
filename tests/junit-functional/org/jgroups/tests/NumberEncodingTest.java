package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.NumberEncoding;
import org.testng.annotations.Test;

@Test(groups=Global.FUNCTIONAL)
public class NumberEncodingTest {

   public static void testEncodeAndDecode() {
      long[] numbers={0, 1, 50, 127, 128, 254, 255, 256,
        Short.MAX_VALUE, Short.MAX_VALUE +1, Short.MAX_VALUE *2, Short.MAX_VALUE *2 +1,
        100000, 500000, 100000,
        Integer.MAX_VALUE, (long)Integer.MAX_VALUE +1, (long)Integer.MAX_VALUE *2, (long)Integer.MAX_VALUE +10,
        Long.MAX_VALUE /10, Long.MAX_VALUE -1, Long.MAX_VALUE};

      for(long num: numbers) {
          byte[] buf=NumberEncoding.encode(num);
          long result=NumberEncoding.decode(buf);
          System.out.println(num + " encoded to " + UtilTest.printBuffer(buf) + " (" + buf.length + " bytes), decoded to " + result);
          assert num == result;
      }
  }

  public static void testSize() {
      int[] shifts={0, 1, 7, 8, 15, 16, 17, 23, 24, 25, 31, 32, 33, 39, 40, 41, 47, 48, 49, 55, 56};

      assert NumberEncoding.size(0) == 1;

      for(int shift: shifts) {
          long num=((long)1) << shift;
          byte size=NumberEncoding.size(num);
          System.out.println(num + " needs " + size + " bytes");
          int num_bytes_required=(shift / 8) +2;
          assert size == num_bytes_required;
      }
  }

}
