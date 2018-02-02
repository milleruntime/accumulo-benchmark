package org.apache;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.tserver.MemKey;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemKeyMap {

    private ConcurrentSkipListMap<Key,Value> map;
    String r = "key1313";
    String cf = "cf1";
    String cq = "cq1";
    MemKey key = new MemKey(r.getBytes(), cf.getBytes(), cq.getBytes(), "".getBytes(), System.currentTimeMillis(), false, false, 10_002);
    Value value = new Value(("val10002").getBytes());

    public MemKeyMap() {
        map = new ConcurrentSkipListMap<>(new MemKeyComparator());
        System.out.println("Creating 10,000 memkeys.");
        for (int i = 1; i < 10_001; i++) {
            String r = "key" + i;
            String cf = "cf" + (i % 5000);
            String cq = "cq" + (i % 1000);
            MemKey mk = new MemKey(r.getBytes(), cf.getBytes(), cq.getBytes(), "".getBytes(), System.currentTimeMillis(), false, false, i);
            map.put(mk, new Value(("val" + i).getBytes()));
        }
        System.out.println("Created Map with size = " + map.size());
    }

    public void put() {
        map.put(key, value);
    }

    public void get() {
        map.get(key);
    }

    class MemKeyComparator implements Comparator<Key>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Key k1, Key k2) {
            int cmp = k1.compareTo(k2);

            if (cmp == 0) {
                if (k1 instanceof MemKey)
                    if (k2 instanceof MemKey)
                        cmp = ((MemKey) k2).getKVCount() - ((MemKey) k1).getKVCount();
                    else
                        cmp = 1;
                else if (k2 instanceof MemKey)
                    cmp = -1;
            }

            return cmp;
        }
    }
}
