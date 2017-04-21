package org.sample;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Created by mpmill4 on 4/24/17.
 */
public class InMemoryMapBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    File testFile = new File(MyBenchmark.testFileName);
    InMemoryMap emptyInMemoryMap = null;
    InMemoryMap inMemoryMap = null;
    SortedMap<Key,Value> sortedMap = null;

    public BenchmarkState() {

      try (Scanner scanner = RFile.newScanner().from(testFile.getAbsolutePath()).withFileSystem(FileSystem.getLocal(new Configuration()))
          .withoutSystemIterators().build()) {
        sortedMap = MyBenchmark.toMap(scanner);
        inMemoryMap = newInMemoryMap(false, "/tmp");
        inMemoryMap.mutate(toMutations(scanner));
        emptyInMemoryMap = newInMemoryMap(false, "/tmp");

      } catch (Exception e) {
        e.printStackTrace();
      }

    }
  }

  public static InMemoryMap newInMemoryMap(boolean useNative, String memDumpDir) throws LocalityGroupUtil.LocalityGroupConfigurationError {
    ConfigurationCopy config = new ConfigurationCopy(DefaultConfiguration.getInstance());
    config.set(Property.TSERV_NATIVEMAP_ENABLED, "" + useNative);
    config.set(Property.TSERV_MEMDUMP_DIR, memDumpDir);
    return new InMemoryMap(config);
  }

  public static List<Mutation> toMutations(Scanner scanner) {
    List<Mutation> mutations = new ArrayList<>();
    for (Map.Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Mutation m = new Mutation(new Text(key.getRow()));
      m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getTimestamp(), entry.getValue());
      mutations.add(m);
    }

    return mutations;
  }

  //TODO: Test not closing InMemoryMap - weird stuff happened before I added close at the end of the test
  @Benchmark
  @Warmup(iterations = 5)
  public void testEmptyInMemoryMap(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600"));
    InMemoryMap.MemoryIterator memoryIterator = state.emptyInMemoryMap.skvIterator(null);
    memoryIterator.seek(range, MyBenchmark.EMPTY_COL_FAMS, false);
    MyBenchmark.readAll(memoryIterator, 8192);
    memoryIterator.close();
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void testInMemoryMap10(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00016610"));
    InMemoryMap.MemoryIterator memoryIterator = state.inMemoryMap.skvIterator(null);
    memoryIterator.seek(range, MyBenchmark.EMPTY_COL_FAMS, false);
    MyBenchmark.readAll(memoryIterator, 8192);
    memoryIterator.close();
  }

  @Benchmark
  @Warmup(iterations = 5)
  public void testInMemoryMap1000(BenchmarkState state) throws IOException {
    Range range = new Range(new Key("mytestrow00016600"), new Key("mytestrow00017600"));
    InMemoryMap.MemoryIterator memoryIterator = state.inMemoryMap.skvIterator(null);
    memoryIterator.seek(range, MyBenchmark.EMPTY_COL_FAMS, false);
    MyBenchmark.readAll(memoryIterator, 8192);
    memoryIterator.close();
  }
}
