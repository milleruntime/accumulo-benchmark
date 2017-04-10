/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sample;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.StatsIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.ImmutableSet;

public class MyBenchmark {

  static final String testFileName = "RfileBenchmarkTest.rf";
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final Collection<ByteSequence> CF1S = new HashSet<>(Arrays.asList(new ArrayByteSequence("testCF1")));
  private static final Collection<ByteSequence> CF2S = new HashSet<>(Arrays.asList(new ArrayByteSequence("testCF2")));

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    File testFile = new File(testFileName);
    SortedMap<Key,Value> sortedMap = null;
    SortedKeyValueIterator<Key,Value> iterStack = null;
    SortedKeyValueIterator<Key,Value> colIterStack = null;

    public BenchmarkState() {
      AtomicLong scannedCount = new AtomicLong();
      AtomicLong seekCount = new AtomicLong();
      KeyExtent extent = new KeyExtent();
      extent.setPrevEndRow(null);
      extent.setEndRow(null);
      AccumuloConfiguration accuConf = AccumuloConfiguration.getDefaultConfiguration();

      System.out.println("Reading rfile into map and creating Iterator stack");
      try (Scanner scanner = RFile.newScanner().from(testFile.getAbsolutePath()).withFileSystem(FileSystem.getLocal(new Configuration())).withoutSystemIterators().build()){
        sortedMap = toMap(scanner);
        List<SortedKeyValueIterator<Key,Value>> sourceIters = new ArrayList<>();
        sourceIters.add(new SortedMapIterator(sortedMap));
        iterStack = createIteratorStack(sourceIters, scannedCount, seekCount, extent, accuConf, Collections.emptySet());
        
        HashSet<Column> columns = new HashSet<>();
        columns.add(new Column("testCF2".getBytes(), "testCQ".getBytes(), null));
        colIterStack = createIteratorStack(sourceIters, scannedCount, seekCount, extent, accuConf, columns);
        
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static SortedMap<Key,Value> toMap(Scanner scanner) {
    TreeMap<Key,Value> map = new TreeMap<>();
    for (Map.Entry<Key,Value> entry : scanner) {
      map.put(entry.getKey(), entry.getValue());
    }
    System.out.println("Read " + testFileName + " First Key = " + map.firstKey());
    return map;
  }

  private static SortedKeyValueIterator<Key,Value> createIteratorStack(List<SortedKeyValueIterator<Key,Value>> sources, AtomicLong scannedCount, AtomicLong seekCount, KeyExtent extent, AccumuloConfiguration accuConf, Set<Column> columns) throws IOException {

    MultiIterator multiIter = new MultiIterator(sources, extent);
    IteratorEnvironment iterEnv = new IteratorEnvironment() {
      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        throw new UnsupportedOperationException();
      }
      public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
        throw new UnsupportedOperationException();
      }
      @Override
      public Authorizations getAuthorizations() {
        return null;
      }
      @Override
      public IteratorEnvironment cloneWithSamplingEnabled() {
        return null;
      }
      @Override
      public boolean isSamplingEnabled() {
        return false;
      }
      @Override
      public SamplerConfiguration getSamplerConfiguration() {
        return null;
      }
      public boolean isFullMajorCompaction() {
        return false;
      }
      public IteratorUtil.IteratorScope getIteratorScope() {
        return IteratorUtil.IteratorScope.scan;
      }
      public AccumuloConfiguration getConfig() {
        return null;
      }
    };

    StatsIterator statsIterator = new StatsIterator(multiIter, seekCount, scannedCount);
    SortedKeyValueIterator<Key,Value> systemIters = IteratorUtil.setupSystemScanIterators(statsIterator, columns, new Authorizations("CHUNKY","CREAMY"), new byte[0]);

    return IteratorUtil.loadIterators(IteratorUtil.IteratorScope.scan, systemIters, extent, accuConf, Collections.emptyList(), Collections.emptyMap(), iterEnv);
  }

  private static void readAll(SortedKeyValueIterator<Key,Value> iter, int expected) throws IOException {
    // int count = 0;
    while(iter.hasTop()) {
      iter.next();
      // count++;
    }
    
    /*
    if(expected != count) {
      throw new IllegalArgumentException(expected + " != "+count);
    }
    */
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
