package org.apache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.hadoop.io.Text;

/**
 * Created by milleruntime on 5/16/17.
 */
public class TestInMemoryStack extends TestIteratorStack {

  // default values
  public static final boolean USE_NATIVE_DEFAULT = false;
  public static final String MEM_DUMP_DIR_DEFAULT = "/tmp";

  protected boolean useNative;
  protected String memDumpDir;
  protected InMemoryMap inMemoryMap = null;
  protected InMemoryMap.MemoryIterator memoryIterator = null;

  public TestInMemoryStack(SortedMap<Key,Value> sortedMap, Authorizations auths, Set<Column> columns) throws Exception {
    this(sortedMap, auths, columns, USE_NATIVE_DEFAULT, MEM_DUMP_DIR_DEFAULT);
  }

  public TestInMemoryStack(SortedMap<Key,Value> sortedMap, Authorizations auths) throws Exception {
    this(sortedMap, auths, Collections.emptySet(), USE_NATIVE_DEFAULT, MEM_DUMP_DIR_DEFAULT);
  }

  public TestInMemoryStack(SortedMap<Key,Value> sortedMap) throws Exception {
    this(sortedMap, Authorizations.EMPTY, Collections.emptySet(), USE_NATIVE_DEFAULT, MEM_DUMP_DIR_DEFAULT);
  }

  public TestInMemoryStack(SortedMap<Key,Value> sortedMap, Authorizations auths, Set<Column> columns, boolean useNative, String memDumpDir) throws Exception {
    super(sortedMap, auths, columns);
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.TSERV_NATIVEMAP_ENABLED, "" + useNative);
    conf.set(Property.TSERV_MEMDUMP_DIR, memDumpDir);
    this.useNative = useNative;
    this.memDumpDir = memDumpDir;
    configuration = conf;
  }

  @Override
  protected SortedKeyValueIterator<Key,Value> createIteratorStack(List<SortedKeyValueIterator<Key,Value>> sources, AtomicLong scannedCount,
      AtomicLong seekCount, KeyExtent extent, AccumuloConfiguration accuConf, Authorizations auths, Set<Column> columns) throws IOException {
    try {
      inMemoryMap =  new InMemoryMap(configuration);
    } catch (LocalityGroupUtil.LocalityGroupConfigurationError e) {
      throw new IOException("Error occurred creating InMemorMap", e);
    }
    inMemoryMap.mutate(toMutations(sortedMap));
    memoryIterator = inMemoryMap.skvIterator(null);
    return memoryIterator;
  }

  public static List<Mutation> toMutations(SortedMap<Key,Value> sortedMap) {
    List<Mutation> mutations = new ArrayList<>();
    for (Map.Entry<Key,Value> entry : sortedMap.entrySet()) {
      Key key = entry.getKey();
      Mutation m = new Mutation(new Text(key.getRow()));
      m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getTimestamp(), entry.getValue());
      mutations.add(m);
    }
    return mutations;
  }
}
