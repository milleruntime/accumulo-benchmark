package org.sample;

import java.util.Map;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Created by mpmill4 on 4/7/17.
 */
public class BenchmarkIT {

  @State(Scope.Benchmark) public static class BenchmarkState {
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
        Authorizations auths = new Authorizations("public");
        scan = conn.createScanner("mytest", auths);
      } catch (Exception e1) {
        e1.printStackTrace();
      }

    }
  }

  @Warmup(iterations = 5)
  @Benchmark
  public void scanTest (BenchmarkState state){
    state.scan.setRange(new Range());
    for(Map.Entry<Key, Value> entry : state.scan) {
      entry.getKey().getRow();
      entry.getValue();
    }
  }
}
