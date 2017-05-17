package org.apache.accumulo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.TestIteratorStack;
import org.apache.commons.cli.Options;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Created by milleruntime on 4/17/17.
 */
public class TestUtils {
  // Add new classes to this array
  // All classes containing Benchmarks
  public static Class [] ALL_CLASSES = { MyBenchmark.class, UnoBenchmarkIT.class, InMemoryMapBenchmark.class, VersioningIterBenchmark.class };

  public static void main(String[] args) {
    for(String a : args)
      switch (a) {
        case "-p":case "--print":
          printTests();
          break;
        case "-v":case "--version":
          printVersion();
          break;
        default:
          System.out.println("Unknown option passed to TestUtils");
      }

  }

  private static void printVersion() {
    System.out.println("Running tests with Accumulo Iterator Stack Version: " + TestIteratorStack.VERSION);
  }

  private static void printTests() {
    List<String> methods = new ArrayList<>();

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
