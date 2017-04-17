package org.sample;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Created by mpmill4 on 4/17/17.
 */
public class PrintTests {

  public static void main(String[] args) {
    List<Method> testMethods = new ArrayList<>();

    // MyBenchmark class
    for(Method method : MyBenchmark.class.getDeclaredMethods()) {
      if(method.isAnnotationPresent(Benchmark.class)) {
        testMethods.add(method);
      }
    }

    // UnoBenchmarkIT
    for(Method method : UnoBenchmarkIT.class.getDeclaredMethods()) {
      if(method.isAnnotationPresent(Benchmark.class)) {
        testMethods.add(method);
      }
    }

    System.out.println("The following methods were found with Benchmarks: ");
    for(Method m : testMethods) {
      System.out.println(m.getName());
    }

  }
}
