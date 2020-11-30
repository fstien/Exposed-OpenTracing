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
    override fun beforeExecution(transaction: Transaction, context: StatementContext) = withActiveSpan {
        log("Starting Execution")

        val query = context.expandArgs(TransactionManager.current())
        setTag("Query", query.sanitize(replacePII))

        setTag("StatementCount", transaction.statementCount)
        setTag("TableNames", context.statement.targets.map { it.tableName }.toString())
        setTag("StatementType", context.statement.type.toString())
    }

    override fun afterExecution(transaction: Transaction, contexts: List<StatementContext>, executedStatement: PreparedStatementApi) = withActiveSpan {
        log("Finished Execution")
    }

    override fun beforeCommit(transaction: Transaction) = withActiveSpan {
        log("Send Commit")
        setTag("Duration", transaction.duration)
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

