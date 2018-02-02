package org.apache.accumulo.benchmark;

import org.apache.accumulo.util.BenchmarkLoader;
import org.apache.accumulo.util.Version;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Runs on 1.7.3 and 1.7.4
 */
public class MemKeyBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        BenchmarkLoader loader;

        public BenchmarkState() {
            try {
                Version.isRequiredVersion(Version.ONE_EIGHT_ONE, Version.ONE_EIGHT_TWO);
                loader = new BenchmarkLoader("org.apache.MemKeyMap");
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    @Benchmark
    @Warmup(iterations = 5)
    public void testPut(BenchmarkState state) throws Exception {
        state.loader.call("put");
    }

    @Benchmark
    @Warmup(iterations = 5)
    public void testGet(BenchmarkState state) throws Exception {
        state.loader.call("get");
    }
}
