package org.apache.accumulo.util;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.AIS;
import org.apache.accumulo.UnoBenchmarkIT;
import org.apache.accumulo.benchmark.*;
import org.apache.accumulo.IteratorStackBenchmark;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Created by milleruntime on 4/17/17.
 */
public class TestUtils {
  // Add new classes to this array
  // All classes containing Benchmarks
  public static Class [] ALL_CLASSES = { TablesBenchmark.class, IteratorStackBenchmark.class, UnoBenchmarkIT.class,
          InMemoryMapBenchmark.class, VersioningIterBenchmark.class, MemKeyBenchmark.class };

  public static void main(String[] args) {
    try {
      for (String a : args)
        switch (a) {
          case "-p":
          case "--print":
            printTests();
            break;
          case "-v":
          case "--version":
            printVersion();
            break;
          default:
            System.out.println("Unknown option passed to TestUtils");
        }
    }catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void printVersion() {
    System.out.println("Running tests with Accumulo Iterator Stack Version: " + AIS.VERSION);
  }

  private static void printTests() throws Exception {
    List<String> methods = new ArrayList<>();
    // dynamically loading classes from a package/directory seems impossible
/*    List<Class> classes = new ArrayList<>();

  File benchmarkDir = Paths.get("", "jmh-test", "target", "classes", "org", "apache", "accumulo", "benchmark").toAbsolutePath().toFile();
    for (File file : benchmarkDir.listFiles()){
      if(file.getName().endsWith(".class")) {
        //classes.add(TestUtils.class.getClassLoader().loadClass("org.apache.accumulo.benchmark." + file.getName()));
        classes.add(Class.forName("org.apache.accumulo.benchmark." + file.getName()));
      }
    } */
    //System.out.println("Found classes: " + classes.size());

    //for(Class clazz : classes) {
    for(Class clazz : ALL_CLASSES) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (method.isAnnotationPresent(Benchmark.class)) {
          methods.add(clazz.getSimpleName() +"."+ method.getName());
        }
      }
    }

    System.out.println("The following methods were found with Benchmarks: ");
    for(String method : methods) {
      System.out.println(method);
    }
  }
}
