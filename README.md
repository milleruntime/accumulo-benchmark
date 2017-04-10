# Accumulo System Iterator Perfomance Tests
---

## Build

```bash
mvn clean package
```

## Generate data

```bash
java -cp target/benchmarks.jar org.sample.Generate

```
## Run test


```bash
java -jar target/benchmarks.jar -i 100 -f 1

```
