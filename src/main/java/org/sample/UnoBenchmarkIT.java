package org.sample;

import java.util.Map;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Test will only work with local Uno installed and running
 * Once Uno is running, then make sure to setauths:
 * $ACCUMULO_HOME/bin/accumulo shell -u root -p secret -e "setauths -u root -s PUBLIC"
 */
public class UnoBenchmarkIT {
  public static final String TEST_TABLE = "mytest";
  public static final String COL_VIS = "PUBLIC";

  public static String colStr(int c) {
    return String.format("%08x", c);
  }


  public static void createTable(BenchmarkState state) throws Exception {
    try {
      state.conn.tableOperations().create(TEST_TABLE, new NewTableConfiguration());
    } catch (TableExistsException e) {
      System.out.println("Table already exits.");
      return;
    }
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(10000000L);
    BatchWriter bw = state.conn.createBatchWriter(TEST_TABLE, config);

    for (int i = 0; i < 1_000_000; i++) {
      String rowId = "mytestrow" + colStr(i);
      Text colFam = new Text("myColFam" + (i % 2));
      Text colQual = new Text("myColQual");
      Value value = new Value(("" + rowId.hashCode()).getBytes());
      ColumnVisibility colVis = new ColumnVisibility(COL_VIS);
      long timestamp = System.currentTimeMillis();
      Mutation m = new Mutation(rowId);
      m.put(colFam, colQual, colVis, timestamp, value);
      bw.addMutation(m);
    }
    System.out.println("Done creating table");
    bw.close();

  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    String instanceName = "uno";
    String zooServers = "localhost";
    Instance inst = null;
    Scanner scan = null;
    Connector conn = null;

    public BenchmarkState() {
      System.out.println("Setting up Accumulo connection. ");
      try {
        inst = new ZooKeeperInstance(instanceName, zooServers);
        conn = inst.getConnector("root", new PasswordToken("secret"));
        createTable(this);
        Authorizations auths = new Authorizations(COL_VIS);
        scan = conn.createScanner(TEST_TABLE, auths);
      } catch (Exception e1) {
        e1.printStackTrace();
      }
    }
  }

  /**
   * Test will only work with local Uno installed an running
   */
  @Warmup(iterations = 5)
  @Benchmark
  public void scanTest (BenchmarkState state){
    state.scan.setRange(new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600")));
    for(Map.Entry<Key, Value> entry : state.scan) {
      entry.getKey().getRow();
      entry.getValue();
    }
  }
}
