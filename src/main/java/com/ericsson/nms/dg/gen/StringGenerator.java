package com.ericsson.nms.dg.gen;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 21/10/13
 * Time: 17:17
 */
public class StringGenerator {

  public int[] generate(final int meanLenght, final int minLength, final int maxLength, final int cardinality) {
    int calc_sum = 0;
    final int[] lengthsForAvarage = new int[cardinality];
    for (int i = 0; i < cardinality; i++) {
      double next = meanLenght * (i + 1) - calc_sum;
      if (i == cardinality - 1) {
        next = Math.ceil(next);
      } else {
        int sd = meanLenght / 4;
        double tmin = next - sd;
        if (tmin < minLength) {
          tmin = minLength;
        }
        double tmax = next + sd;
        if (tmax > maxLength) {
          tmax = maxLength;
        }
        next = tmin + (int) (Math.random() * ((tmax - tmin) + 1));
      }
      calc_sum += next;
      if (next < minLength) {
        throw new IndexOutOfBoundsException(next + " < " + minLength);
      }
      lengthsForAvarage[i] = (int) next;
      calculatedValue(lengthsForAvarage[i]);
    }
    return lengthsForAvarage;
  }

  public void calculatedValue(final int length) {
    // default is do nothing...
  }

}
