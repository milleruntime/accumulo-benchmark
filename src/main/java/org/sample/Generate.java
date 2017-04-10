package org.sample;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class Generate {
  private static String colStr(int c) {
    return String.format("%08x", c);
  }
  public static void main(String[] args) throws IOException {
    System.out.println("Populating RFile");
    File testFile = new File(MyBenchmark.testFileName);
    RFileWriter writer = RFile.newWriter().to(testFile.getAbsolutePath()).withFileSystem(FileSystem.getLocal(new Configuration())).build();
    String[] visibilities = new String[]{"","CHUNKY","CREAMY","CHUNKY|CREAMY"};
    for (int i = 0; i < 1_000_000; i++) {
      String cv = visibilities[i % visibilities.length];
      Key k1 = new Key("mytestrow" + colStr(i), "testCF1", "", cv);
      Key k2 = new Key("mytestrow" + colStr(i), "testCF2", "testCQ", cv);
      writer.append(k1, new Value((k1.hashCode() + "").getBytes()));
      writer.append(k2, new Value((k2.hashCode() + "").getBytes()));
    }
    writer.close();
  }
}
