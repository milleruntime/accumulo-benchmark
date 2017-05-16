package org.apache.accumulo;

import java.io.IOException;
import java.util.SortedMap;
import org.apache.TestInMemoryStack;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Created by milleruntime on 4/24/17.
 */
public class InMemoryMapBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    SortedKeyValueIterator<Key,Value> memoryIterator = null;
    SortedMap<Key,Value> sortedMap = null;

    public BenchmarkState() {
      try {
        sortedMap = MyBenchmark.generateSortedMap(new String[]{"","CHUNKY","CREAMY","CHUNKY|CREAMY"});
        memoryIterator =  new TestInMemoryStack(sortedMap).getIterStack();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Benchmark
  @Warmup(iterations = 10)
  public void testInMemoryMap10(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00016610"));
    state.memoryIterator.seek(range, MyBenchmark.EMPTY_COL_FAMS, false);
    MyBenchmark.readAll(state.memoryIterator, 8192);
    //memoryIterator.close();
  }

  @Benchmark
  @Warmup(iterations = 10)
  public void testInMemoryMap1000(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600"));
    state.memoryIterator.seek(range, MyBenchmark.EMPTY_COL_FAMS, false);
    MyBenchmark.readAll(state.memoryIterator, 8192);
    //memoryIterator.close();
  }
}
