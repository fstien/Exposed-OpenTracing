package com.github.fstien.exposed.opentracing

import io.opentracing.util.GlobalTracer
import mu.KotlinLogging
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction


private val log = KotlinLogging.logger {}

enum class Contains { PII, NoPII }
val NoPII = Contains.NoPII
val PII = Contains.PII

fun <T> tracedTransaction(
        vararg replacePII: String = emptyArray(),
        contains: Contains = Contains.PII,
        statement: Transaction.() -> T): T {

    if (!validInputs(replacePII.toList(), contains)) {
        log.warn("Invalid inputs to tracedTransaction(), no span created.")
        return transaction { statement() }
    }

    val tracer = GlobalTracer.get()
    if (tracer == null) {
        log.warn("Tracer not found in GlobalTracer, no span created.")
        return transaction { statement() }
    }

    val span = tracer.buildSpan("ExposedTransaction").start()
    try {
        tracer.scopeManager().activate(span).use { scope ->
            return transaction {
                registerInterceptor(OpenTracingInterceptor(replacePII.toList()))
                statement()
            }
        }
    } finally {
        span.finish()
    }
}
