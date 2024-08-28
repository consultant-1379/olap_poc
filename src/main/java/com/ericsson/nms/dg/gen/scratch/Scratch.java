package com.ericsson.nms.dg.gen.scratch;

import java.security.MessageDigest;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 29/11/13
 * Time: 18:05
 */
public class Scratch {
  public static void main(String[] args) throws Exception {
    MessageDigest messageDigest = MessageDigest.getInstance("MD5");
    for (int i = 0; i < 20; i++) {
      System.out.println(new String(messageDigest.digest()));
    }
  }
}
