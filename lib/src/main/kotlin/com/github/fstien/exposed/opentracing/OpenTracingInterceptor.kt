package com.github.fstien.exposed.opentracing

import io.opentracing.Span
import io.opentracing.util.GlobalTracer
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.statements.StatementContext
import org.jetbrains.exposed.sql.statements.StatementInterceptor
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import org.jetbrains.exposed.sql.statements.expandArgs
import org.jetbrains.exposed.sql.transactions.TransactionManager


internal class OpenTracingInterceptor(private val replacePII: List<String>): StatementInterceptor {
    companion object {
        val durations = mutableMapOf<String, Long?>()
    }

    override fun beforeExecution(transaction: Transaction, context: StatementContext) = withActiveSpan {
        val query = context.expandArgs(TransactionManager.current())

        log(mapOf(
            "event" to "Starting Execution",
            "query" to query.sanitize(replacePII),
            "tables" to context.statement.targets.map { it.tableName }.toString(),
        ))

        setTag("StatementCount", transaction.statementCount)
        setTag("DbUrl", transaction.db.url)
        setTag("DbVendor", transaction.db.vendor)
        setTag("DbVersion", transaction.db.version)

        durations[transaction.id] = System.currentTimeMillis()
    }

    override fun afterExecution(transaction: Transaction, contexts: List<StatementContext>, executedStatement: PreparedStatementApi) = withActiveSpan {
        var duration = "NA"
        val startTime = durations[transaction.id]
        if (startTime != null) {
            duration = (System.currentTimeMillis() - startTime).toString()
            durations[transaction.id] = null
        }

        log(mapOf(
                "event" to "Finished Execution",
                "duration" to duration
        ))
    }

    override fun beforeCommit(transaction: Transaction) = withActiveSpan {
        log("Send Commit")
    }

    override fun afterCommit() = withActiveSpan {
        log("Transaction Committed")
    }

    override fun beforeRollback(transaction: Transaction) = withActiveSpan {
        log("Before Rollback")
    }

    override fun afterRollback() = withActiveSpan {
        log("After Rollback")
    }
}

internal inline fun withActiveSpan(block: Span.() -> Unit) {
    val activeSpan: Span = GlobalTracer.get().activeSpan() ?: return
    with(activeSpan) {
        block()
    }
}

internal fun String.sanitize(replace: List<String>): String {
    var returnString = this

    for (str in replace) {
        returnString = returnString.replace(oldValue = str, newValue = "<REDACTED>")
    }

    return returnString
}

