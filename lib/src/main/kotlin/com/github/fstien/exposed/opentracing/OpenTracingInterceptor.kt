package com.github.fstien.exposed.opentracing

import io.opentracing.References
import io.opentracing.Scope
import io.opentracing.Span
import io.opentracing.Tracer
import io.opentracing.util.GlobalTracer
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.statements.StatementContext
import org.jetbrains.exposed.sql.statements.StatementInterceptor
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import org.jetbrains.exposed.sql.statements.expandArgs
import org.jetbrains.exposed.sql.transactions.TransactionManager


internal class OpenTracingInterceptor(private val replacePII: List<String>): StatementInterceptor {
    private val tracer: Tracer = GlobalTracer.get()

    companion object {
        val scopes = mutableMapOf<String, Scope>()
    }

    override fun beforeExecution(transaction: Transaction, context: StatementContext) {

        tracer.activeSpan()?.let { transactionSpan ->
            transactionSpan.setTag("StatementCount", transaction.statementCount)
            transactionSpan.setTag("DbUrl", transaction.db.url)
            transactionSpan.setTag("DbVendor", transaction.db.vendor)
            transactionSpan.setTag("DbVersion", transaction.db.version)
        }

        val span = tracer.buildSpan("ExposedQuery").start()
        val scope = tracer.scopeManager().activate(span)
        scopes[transaction.id] = scope

        val query = context.expandArgs(TransactionManager.current())
            .sanitize(replacePII)
            .format()

        span.setTag("query", query)
        span.setTag("table", context.statement.targets.map { it.tableName }.toString())
    }

    override fun afterExecution(
        transaction: Transaction,
        contexts: List<StatementContext>,
        executedStatement: PreparedStatementApi
    ) {
        val span = tracer.scopeManager().activeSpan()
        span.finish()

        with(scopes[transaction.id]) {
            this?.close()
        }
    }

    override fun beforeCommit(transaction: Transaction) = withActiveSpan(tracer) {
        log("Send Commit")
    }

    override fun afterCommit() = withActiveSpan(tracer) {
        log("Transaction Committed")
    }

    override fun beforeRollback(transaction: Transaction) = withActiveSpan(tracer) {
        log("Before Rollback")
    }

    override fun afterRollback() = withActiveSpan(tracer) {
        log("After Rollback")
    }
}

internal inline fun withActiveSpan(tracer: Tracer, block: Span.() -> Unit) {
    val activeSpan: Span = tracer.activeSpan() ?: return
    block(activeSpan)
}
