package org.sample;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Random;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

public class VersioningIterBenchmark {
  @State(Scope.Benchmark)
  public static class BenchmarkState {
    
    SortedKeyValueIterator<Key,Value> iterStack = null;
   
    public BenchmarkState() {
      Random rand = new Random(42);
      
      TreeMap<Key, Value> tmap = new TreeMap<>();
      
      int v = 0;
      
      for(int r = 0; r < 100; r++) {
        
        String row = String.format("%09d", rand.nextInt(1_000_000_000));
        for(int q = 0; q < 3; q++) {
          String qual = String.format("%06d", rand.nextInt(1_000_000));
          int stamps = rand.nextInt(19)+1;
          for(int t = 0; t< stamps; t++) {
            Key k = new Key(row, "fam",qual, rand.nextInt(1_000_000_000));
            Value val = new Value(v++ + "");
            
            tmap.put(k, val);
          }
        }
      }
      
      System.out.println("tmap.size : "+tmap.size());
      
      SortedMapIterator smi = new SortedMapIterator(tmap);
      VersioningIterator vi = new VersioningIterator();
      try {
        vi.init(smi, Collections.emptyMap(), null);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      
      iterStack = vi;
    }
  }
  
  @Benchmark
  @Warmup(iterations = 5)
  public void scanAll(BenchmarkState state) throws IOException {
    SortedKeyValueIterator<Key,Value> iter = state.iterStack;
    iter.seek(new Range(), Collections.emptySet(), false);
    
    while(iter.hasTop()) {
      iter.next();
    }
  }
  
}
