package com.github.fstien.exposed.opentracing

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import com.github.fstien.exposed.opentracing.util.DatabaseTestsBase
import com.github.fstien.exposed.opentracing.util.Person
import com.github.fstien.exposed.opentracing.util.TestDB
import com.zopa.ktor.opentracing.ThreadContextElementScopeManager
import io.opentracing.mock.MockTracer
import io.opentracing.util.GlobalTracer
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance


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

        with(mockTracer.finishedSpans()) {
            val parentSpan = firstOrNull { it.operationName() == "parent-span" }!!
            val transactionSpan = firstOrNull { it.operationName() == "ExposedTransaction" }!!

            assertThat(parentSpan.context()?.traceId()).isEqualTo(transactionSpan.context()?.traceId())
            assertThat(transactionSpan.parentId()).isEqualTo(parentSpan.context()?.spanId())

            with(transactionSpan.tags()) {
                assertThat(get("DbUrl")).isEqualTo("jdbc:sqlite:file:test?mode=memory&cache=shared")
                assertThat(get("DbVendor")).isEqualTo("sqlite")
                assertThat(this["StatementCount"]).isEqualTo(3)
            }

            val query1Span = firstOrNull { it.operationName() == "ExposedQuery" }!!
            val query2Span = lastOrNull { it.operationName() == "ExposedQuery" }!!

            with(query1Span.tags()) {
                assertThat(this["query"]).isEqualTo(
                    """SELECT
                      |        Person.id,
                      |        Person."name",
                      |        Person.age,
                      |        Person.password 
                      |    FROM
                      |        Person 
                      |    WHERE
                      |        Person."name" = 'Francois'""".trimMargin())

                assertThat(this["table"]).isEqualTo("[Person]")
            }

            with(query2Span.tags()) {
                assertThat(this["query"]).isEqualTo(
                     """INSERT 
                        |    INTO
                        |        Person
                        |        (age, "name", password) 
                        |    VALUES
                        |        (25, 'Francois', 'OWIDJFedw')""".trimMargin())
                assertThat(this["table"]).isEqualTo("[Person]")
            }
        }
    }

    @Test
    fun `tracedTransaction with PII sanitised query string`() = withTables(Person) {
        val username = "el_franfran"
        val password = "oquejJNLKJQnW"

        withParentSpan {
            tracedTransaction(contains = PII, username, password) {
                Person.insert {
                    it[Person.username] = username
                    it[Person.age] = 12
                    it[Person.password] = password
                }
            }
        }

        with(mockTracer.finishedSpans().first()) {
            assertThat(this.tags()["query"]).isEqualTo(
                """INSERT 
                  |    INTO
                  |        Person
                  |        (age, "name", password) 
                  |    VALUES
                  |        (12, '<REDACTED>', '<REDACTED>')""".trimMargin())
        }
    }

    private inline fun withParentSpan(crossinline block: () -> Any) {
        val span = mockTracer.buildSpan("parent-span").start()

        try {
            mockTracer.scopeManager().activate(span).use { scope ->
                block()
            }
        } finally {
            span.finish()
        }
    }
}