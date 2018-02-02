package org.apache.accumulo.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;


public class TablesBenchmark {
    public static String table = "table";

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        MiniAccumuloCluster mac;
        TableOperations tableOps;
        Connector conn;
        Instance instance;

        public BenchmarkState() {
            try {
                Path miniDir = Paths.get(System.getProperty("user.dir"), "jmh-test", "target", "mini-tests");
                if (!miniDir.toFile().exists())
                    Files.createDirectory(miniDir);
                Path tempDir = Files.createTempDirectory(miniDir, "mac");
                mac = new MiniAccumuloCluster(tempDir.toFile(), "blah");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Setup(Level.Trial)
        public void start() {
            try {
                mac.start();
                conn = mac.getConnector("root", "blah");
                tableOps = conn.tableOperations();
                instance = conn.getInstance();
                System.out.println("Got connections creating 100 tables");
                for(int i = 0; i < 100; i++) {
                    tableOps.create(table + i);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @TearDown(Level.Trial)
        public void shutdown() {
            System.out.println("Shutting down MAC");
            try {
                mac.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }



    @Benchmark
    @Warmup(iterations = 5)
    public void getTableId(TablesBenchmark.BenchmarkState state) throws Exception {
        Tables.getTableId(state.instance, "table33");
    }

    @Benchmark
    @Warmup(iterations = 5)
    public void getTableMap(TablesBenchmark.BenchmarkState state) throws Exception {
        state.tableOps.tableIdMap();
    }

    @Benchmark
    @Warmup(iterations = 5)
    public void getExists(TablesBenchmark.BenchmarkState state) throws Exception {
        state.tableOps.exists("table33");
    }
}
