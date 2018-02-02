package org.apache.accumulo.util;

import java.lang.reflect.Method;

/**
 * This class allows loading classes at runtime that only exists in certain versions.
 */
public class BenchmarkLoader {


    private Class myClass;
    private Object myObject;

    public BenchmarkLoader(String className) throws Exception {
        this.myClass = this.getClass().getClassLoader().loadClass(className);
        System.out.println("Created new BenchmarkLoader for " + this.myClass);
        this.myObject = myClass.newInstance();
    }

    public Object call(String methodName, Object... objects) throws Exception {
        Method m = myClass.getMethod(methodName);
        return m.invoke(myObject, objects);
    }
}
