package org.apache.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.TestIteratorStack;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

public class IteratorStackBenchmark {

  public static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  public static final Collection<ByteSequence> CF1S = new HashSet<>(Arrays.asList(new ArrayByteSequence("testCF1")));
  public static final Collection<ByteSequence> CF2S = new HashSet<>(Arrays.asList(new ArrayByteSequence("testCF2")));

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    SortedMap<Key,Value> sortedMap = null;
    SortedKeyValueIterator<Key,Value> iterStack = null;
    SortedKeyValueIterator<Key,Value> colIterStack = null;
    SortedKeyValueIterator<Key,Value> emptyAuthsIterStack = null;

    public BenchmarkState() {

      System.out.println("Generating data and creating Iterator stack version " + TestIteratorStack.VERSION);
      try {
        Authorizations auths = new Authorizations("CHUNKY", "CREAMY");
        sortedMap = generateSortedMap(new String[]{"","CHUNKY","CREAMY","CHUNKY|CREAMY"});
        iterStack = new TestIteratorStack(sortedMap, auths).getIterStack();
        HashSet<Column> columns = new HashSet<>();
        columns.add(new Column("testCF2".getBytes(), "testCQ".getBytes(), null));
        colIterStack = new TestIteratorStack(sortedMap, auths, columns).getIterStack();
        emptyAuthsIterStack = new TestIteratorStack(sortedMap).getIterStack();

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  protected static String colStr(int c) {
    return String.format("%08x", c);
  }

  public static SortedMap<Key, Value> generateSortedMap(String[] visibilities) {
    TreeMap<Key,Value> map = new TreeMap<>();
    for (int i = 0; i < 1_000_000; i++) {
      String cv = visibilities[i % visibilities.length];
      Key k1 = new Key("mytestrow" + colStr(i), "testCF1", "", cv);
      Key k2 = new Key("mytestrow" + colStr(i), "testCF2", "testCQ", cv);
      map.put(k1, new Value((k1.hashCode() + "").getBytes()));
      map.put(k2, new Value((k2.hashCode() + "").getBytes()));
    }
    return map;
  }

  public static void readAll(SortedKeyValueIterator<Key,Value> iter, int expected) throws IOException {
    //int count = 0;
    while (iter.hasTop()) {
      iter.next();
      //count++;
    }

    //if(expected != count) { throw new IllegalArgumentException(expected + " != "+count); }
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void seekTestWithCfFilter10(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00016610"));
    state.iterStack.seek(range, CF1S, true);
    readAll(state.iterStack, 16);
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void seekTestWithCfFilter1000(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600"));
    state.iterStack.seek(range, CF1S, true);
    readAll(state.iterStack, 4096);
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void seekTestWithCQFilter10(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00016610"));
    state.colIterStack.seek(range, CF2S, true);
    readAll(state.colIterStack, 16);
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void seekTestWithCQFilter1000(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600"));
    state.colIterStack.seek(range, CF2S, true);
    readAll(state.colIterStack, 4096);
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void seekTestWithEmptyAuths10(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00016610"));
    state.emptyAuthsIterStack.seek(range, EMPTY_COL_FAMS, false);
    readAll(state.emptyAuthsIterStack, 8);
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void seekTestWithEmptyAuths1000(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600"));
    state.emptyAuthsIterStack.seek(range, EMPTY_COL_FAMS, false);
    readAll(state.emptyAuthsIterStack, 2048);
  }
  
  @Benchmark
  @Warmup(iterations = 5)
  public void seekAllSystemIters10(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00016610"));
    state.iterStack.seek(range, EMPTY_COL_FAMS, false);
    readAll(state.iterStack, 32);
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void seekAllSystemIters1000(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600"));
    state.iterStack.seek(range, EMPTY_COL_FAMS, false);
    readAll(state.iterStack, 8192);
  }

}
