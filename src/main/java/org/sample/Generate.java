package org.sample;

import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class Generate {
  private static String colStr(int c) {
    return String.format("%08x", c);
  }

  public static void main(String[] args) {
    System.out.println("Not supported in version 1.7");
  }

  public static SortedMap<Key, Value> populateMap() {
    Random rand = new Random(42);

    TreeMap<Key,Value> tmap = new TreeMap<>();
    int v = 0;
    for (int r = 0; r < 100; r++) {
      String row = String.format("%09d", rand.nextInt(1_000_000_000));
      for (int q = 0; q < 3; q++) {
        String qual = String.format("%06d", rand.nextInt(1_000_000));
        int stamps = rand.nextInt(19) + 1;
        for (int t = 0; t < stamps; t++) {
          Key k = new Key(row, "fam", qual, rand.nextInt(1_000_000_000));
          Value val = new Value((v++ + "").getBytes());
          tmap.put(k, val);
        }
      }
    }
    return tmap;
  }

}
