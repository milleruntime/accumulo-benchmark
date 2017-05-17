# Accumulo System Iterator Perfomance Tests
---

## Build Version 2.0.0 (Default)

```bash
mvn clean install
```
## Build other versions
```bash
mvn clean install -P 173
mvn clean install -P 174
```

## Run all tests
```bash
./run
```

## Run specific tests
```bash
./run -t testMethodName*
```
Regex can be used for test-name to run multiple tests at once.

## Run a single test
```bash
./run -t ClassName.testMethodName
```
## Specify the number of iterations (Default = 20)
```bash
./run -t ClassName.testMethodName -i 100
```
The output of the tests run will be saved in the results directory.

## Print all benchmark tests
This is handy for quickly seeing all the tests available.
```bash
./run -p
```
