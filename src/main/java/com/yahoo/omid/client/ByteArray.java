package com.yahoo.omid.client;

import java.util.Arrays;

public class ByteArray {
   public byte [] array;

   public ByteArray(byte [] array) {
      this.array = array;
   }
   
   public ByteArray(byte [] ... arrays) {
      int size = 0;
      for (byte[] array : arrays) {
         size += array.length;
      }
      this.array = new byte[size];
      size = 0;
      for (byte[] array : arrays) {
         System.arraycopy(array, 0, this.array, size, array.length);
         size += array.length;
      }
   }
   
   @Override
   public boolean equals(Object obj) {
      if (obj instanceof ByteArray) {
         ByteArray ba = (ByteArray) obj;
         return Arrays.equals(array, ba.array);
      }
      return false;
   }
   
   @Override
   public int hashCode() {
      return Arrays.hashCode(array);
   }
}