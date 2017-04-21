package org.sample;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Created by mpmill4 on 4/17/17.
 */
public class PrintTests {
  // Add new classes to this array
  // All classes containing Benchmarks
  public static Class [] ALL_CLASSES = { MyBenchmark.class, UnoBenchmarkIT.class, InMemoryMapBenchmark.class };

  public static void main(String[] args) {
    List<Method> testMethods = new ArrayList<>();

    for(Class clazz : ALL_CLASSES) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (method.isAnnotationPresent(Benchmark.class)) {
          testMethods.add(method);
        }
      }
    }

    System.out.println("The following methods were found with Benchmarks: ");
    for(Method m : testMethods) {
      System.out.println(m.getName());
    }

  }
}
