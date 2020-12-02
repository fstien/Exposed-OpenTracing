package com.github.fstien.exposed.opentracing

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import com.github.fstien.exposed.opentracing.util.DatabaseTestsBase
import com.github.fstien.exposed.opentracing.util.Person
import com.github.fstien.exposed.opentracing.util.TestDB
import com.zopa.ktor.opentracing.ThreadContextElementScopeManager
import com.zopa.ktor.opentracing.threadLocalSpanStack
import io.opentracing.Span
import io.opentracing.mock.MockTracer
import io.opentracing.util.GlobalTracer
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExposedOpenTracingTest: DatabaseTestsBase() {
    private val mockTracer = MockTracer(ThreadContextElementScopeManager())

    @BeforeEach
    fun setUp() {
        TestDB.SQLITE.connect()
        mockTracer.reset()
        GlobalTracer.registerIfAbsent(mockTracer)
    }

    @Test
    fun `tracedTransaction calls lambda expression if no other parameters passed`() {
        var statementRan = false

        tracedTransaction {
            statementRan = true
        }

        assertThat(statementRan).isTrue()
    }

    @Test
    fun `validInputs returns false if empty list and PII passed`() {
        val result = validInputs(contains = Contains.PII, replacePii = emptyList())

        assertThat(result).isFalse()
    }

    @Test
    fun `validInputs returns false if non empty list and NoPII passed`() {
        val result = validInputs(contains = Contains.NoPII, replacePii = listOf("password"))

        assertThat(result).isFalse()
    }

    @Test
    fun `validInputs returns true for non empty list and PII passed`() {
        val result = validInputs(contains = Contains.PII, replacePii = listOf("password"))

        assertThat(result).isTrue()
    }

    @Test
    fun `validInputs returns true for empty list and NoPII passed`() {
        val result = validInputs(contains = Contains.NoPII, replacePii = emptyList())

        assertThat(result).isTrue()
    }

    @Test
    fun `tracedTransaction creates child span`() = withTables(Person) {
        transaction {
            SchemaUtils.create(Person)
        }

        runBlocking {
            withParentSpan {
                tracedTransaction(contains = NoPII) {
                    val person = Person
                        .select { Person.username eq "Francois" }
                        .firstOrNull()

                    if (person == null) {
                        Person.insert {
                            it[username] = "Francois"
                            it[age] = 25
                            it[password] = "OWIDJFedw"
                        }
                    }
                }
            }
        }

        with(mockTracer.finishedSpans()) {
            assertThat(first().context().traceId()).isEqualTo(last().context().traceId())
            assertThat(first().parentId()).isEqualTo(last().context().spanId())

            with(first()) {
                assertThat(this.operationName()).isEqualTo("ExposedTransaction")

                with(tags()) {
                    assertThat(this["DbUrl"]).isEqualTo("jdbc:sqlite:file:test?mode=memory&cache=shared")
                    assertThat(this["DbVendor"]).isEqualTo("sqlite")
                    assertThat(this["StatementCount"]).isEqualTo(3)
                }

                with(logEntries()) {
                    assertThat(this[0].fields()["event"]).isEqualTo("Starting Execution")
                    assertThat(this[0].fields()["query"]).isEqualTo("SELECT Person.id, Person.\"name\", Person.age, Person.password FROM Person WHERE Person.\"name\" = 'Francois'")
                    assertThat(this[0].fields()["tables"]).isEqualTo("[Person]")

                    assertThat(this[1].fields()["event"]).isEqualTo("Finished Execution")

                    assertThat(this[2].fields()["event"]).isEqualTo("Starting Execution")
                    assertThat(this[2].fields()["query"]).isEqualTo("INSERT INTO Person (age, \"name\", password) VALUES (25, 'Francois', 'OWIDJFedw')")
                    assertThat(this[2].fields()["tables"]).isEqualTo("[Person]")

                    assertThat(this[3].fields()["event"]).isEqualTo("Finished Execution")
                }
            }
        }
    }

    @Test
    fun `tracedTransaction with PII santitises query string`() = withTables(Person) {
        val username = "el_franfran"
        val password = "oquejJNLKJQnW"

        runBlocking {
            withParentSpan {
                tracedTransaction(contains = PII, username, password) {
                    Person.insert {
                        it[Person.username] = username
                        it[Person.age] = 12
                        it[Person.password] = password
                    }
                }
            }
        }

        with(mockTracer.finishedSpans().first()) {
            assertThat(this.logEntries().first().fields()["query"]).isEqualTo("INSERT INTO Person (age, \"name\", password) VALUES (12, '<REDACTED>', '<REDACTED>')")
        }
    }

    private suspend inline fun withParentSpan(crossinline block: () -> Any) {
        val span = mockTracer.buildSpan("parent-span").start()
        val spanStack = Stack<Span>()
        spanStack.push(span)

        withContext(threadLocalSpanStack.asContextElement(spanStack)) {
            try {
                mockTracer.scopeManager().activate(span).use { scope ->
                    block()
                }
            } finally {
                span.finish()
            }
        }
    }
}