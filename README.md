<img src="./img/logo.png" alt="Exposed" height="200" />

![Maven Central](https://img.shields.io/maven-central/v/com.github.fstien/exposed-opentracing?color=green)
![GitHub](https://img.shields.io/github/license/fstien/Exposed-OpenTracing.svg?color=green&style=popout)
[![Unit Tests Actions Status](https://github.com/fstien/Exposed-OpenTracing/workflows/Unit%20Tests/badge.svg)](https://github.com/{userName}/{repoName}/actions)

# Exposed OpenTracing

[OpenTracing](https://opentracing.io/) instrumentation of [Exposed](https://github.com/JetBrains/Exposed). Observe database transactions with spans tagged with query strings, table names and more. Logs execution start/ending, transaction commit and rollback. Santise queries to safeguard PII.

## Usage

In an application with a tracer registered in [GlobalTracer](https://opentracing.io/guides/java/tracers/#global-tracer), replace your [Exposed](https://github.com/JetBrains/Exposed) `transaction` with a `tracedTransaction`:

```kotlin
tracedTransaction(contains = NoPII) {
    Cities.insert {
        it[name] = "St. Petersburg"
    } 
}
```
The execution will be wrapped with a child [span](https://opentracing.io/docs/overview/spans/) of the previously active span, which will be tagged with the SQL query. If your query contains PII that you do not want to leak to the tracing system, pass the sensitive strings to the call as follows:
```kotlin
tracedTransaction(contains = PII, name, password) {
    Users.insert {
        it[Users.username] = username
        it[Users.name] = name
        it[Users.password] = password
    } 
}
```
The `name` and `password` strings with be replaced with `<REDACTED>` in the query tagged on the span.
If no strings are password with `contains = PII` or if a string is passed with `contains = NoPII`, a warn log will be written, and the transaction will execute without tracing. 

The resulting `ExposedTransaction` span looks as follows in [Jaeger](https://www.jaegertracing.io/):

![](./img/jaeger2.png)

## Installation

From [Maven Central](https://search.maven.org/artifact/com.github.fstien/exposed-opentracing).
### Maven
Add the following dependency to your `pom.xml`:
    
    <dependency>
      <groupId>com.github.fstien</groupId>
      <artifactId>exposed-opentracing</artifactId>
      <version>VERSION_NUMBER</version>
    </dependency>

### Gradle
Add the following to your dependencies in your `build.gradle`:

    implementation 'com.github.fstien:exposed-opentracing:VERSION_NUMBER'

## Example 

For an example Ktor application, see [Exposed-OpenTracing-example](https://github.com/fstien/Exposed-OpenTracing-example).
