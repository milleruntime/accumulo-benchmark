package org.apache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.StatsIterator;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Class used for testing Accumulo Iterator Stack
 */
public class TestIteratorStack {
  public static final String VERSION = "2.0.0";
  protected SortedKeyValueIterator<Key,Value> iterStack = null;
  protected AtomicLong scannedCount;
  protected AtomicLong seekCount;
  protected KeyExtent extent;
  protected AccumuloConfiguration configuration;

  protected SortedMap<Key,Value> sortedMap;
  protected Authorizations auths;
  protected Set<Column> columns;

  public TestIteratorStack() {
    throw new UnsupportedOperationException();
  }

  public TestIteratorStack(SortedMap<Key,Value> sortedMap, Authorizations auths, Set<Column> columns) throws IOException {
    scannedCount = new AtomicLong();
    seekCount = new AtomicLong();
    extent = new KeyExtent();
    extent.setPrevEndRow(null);
    extent.setEndRow(null);
    configuration = DefaultConfiguration.getInstance();

    this.sortedMap = sortedMap;
    this.auths = auths;
    this.columns = columns;
  }

  public TestIteratorStack(SortedMap<Key,Value> sortedMap, Authorizations auths) throws IOException {
    this(sortedMap, auths, Collections.emptySet());
  }

  public TestIteratorStack(SortedMap<Key,Value> sortedMap) throws IOException {
    this(sortedMap, Authorizations.EMPTY, Collections.emptySet());
  }

  protected SortedKeyValueIterator<Key,Value> createIteratorStack(List<SortedKeyValueIterator<Key,Value>> sources, AtomicLong scannedCount,
      AtomicLong seekCount, KeyExtent extent, AccumuloConfiguration accuConf, Authorizations auths, Set<Column> columns) throws IOException {

    MultiIterator multiIter = new MultiIterator(sources, extent);
    IteratorEnvironment iterEnv = new IteratorEnvironment() {
      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        throw new UnsupportedOperationException();
      }

      public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
        throw new UnsupportedOperationException();
      }

      @Override public Authorizations getAuthorizations() {
        return null;
      }
      @Override public IteratorEnvironment cloneWithSamplingEnabled() {
        return null;
      }
      @Override public boolean isSamplingEnabled() {
        return false;
      }
      @Override public SamplerConfiguration getSamplerConfiguration() {
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
    SortedKeyValueIterator<Key,Value> systemIters = IteratorUtil.setupSystemScanIterators(statsIterator, columns, auths, new byte[0]);

    return IteratorUtil.loadIterators(IteratorUtil.IteratorScope.scan, systemIters, extent, accuConf, Collections.emptyList(), Collections.emptyMap(), iterEnv);
  }

  /**
   * Create the iterator stack and return it.
   */
  public SortedKeyValueIterator<Key,Value> getIterStack() throws IOException {
    if(iterStack == null) {
      List<SortedKeyValueIterator<Key,Value>> sourceIters = new ArrayList<>();
      sourceIters.add(new SortedMapIterator(sortedMap));
      iterStack = createIteratorStack(sourceIters, scannedCount, seekCount, extent, configuration, this.auths, this.columns);
    }
    return iterStack;
  }

  public AtomicLong getScannedCount() {
    return scannedCount;
  }

  public AtomicLong getSeekCount() {
    return seekCount;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  public AccumuloConfiguration getConfiguration() {
    return configuration;
  }

  public SortedMap<Key,Value> getSortedMap() {
    return sortedMap;
  }

  public Authorizations getAuths() {
    return auths;
  }

  public Set<Column> getColumns() {
    return columns;
  }
}
