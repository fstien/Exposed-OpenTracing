package com.github.fstien.exposed.opentracing.util

import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.transactions.transactionManager
import java.util.*
import kotlin.concurrent.thread


enum class TestDB(val connection: () -> String, val driver: String, val user: String = "root", val pass: String = "",
                  val beforeConnection: () -> Unit = {}, val afterTestFinished: () -> Unit = {}, var db: Database? = null) {
    SQLITE({"jdbc:sqlite:file:test?mode=memory&cache=shared"}, "org.sqlite.JDBC");

    fun connect() = Database.connect(connection(), user = user, password = pass, driver = driver)

    companion object {
        fun enabledInTests(): List<TestDB> {
            val embeddedTests = (TestDB.values().toList()).joinToString()
            val concreteDialects = System.getProperty("exposed.test.dialects", embeddedTests).let {
                if (it == "") emptyList()
                else it.split(',').map { it.trim().toUpperCase() }
            }
            return values().filter { concreteDialects.isEmpty() || it.name in concreteDialects }
        }
    }
}

private val registeredOnShutdown = HashSet<TestDB>()

abstract class DatabaseTestsBase {
    init {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    }
    fun withDb(dbSettings: TestDB, statement: Transaction.(TestDB) -> Unit) {
        if (dbSettings !in TestDB.enabledInTests()) {
            exposedLogger.warn("$dbSettings is not enabled for being used in tests", RuntimeException())
            return
        }

        if (dbSettings !in registeredOnShutdown) {
            dbSettings.beforeConnection()
            Runtime.getRuntime().addShutdownHook(thread(false){
                dbSettings.afterTestFinished()
                registeredOnShutdown.remove(dbSettings)
            })
            registeredOnShutdown += dbSettings
            dbSettings.db = dbSettings.connect()
        }

        val database = dbSettings.db!!

        transaction(database.transactionManager.defaultIsolationLevel, 1, db = database) {
            statement(dbSettings)
        }
    }

    fun withDb(db : List<TestDB>? = null, excludeSettings: List<TestDB> = emptyList(), statement: Transaction.(TestDB) -> Unit) {
        val enabledInTests = TestDB.enabledInTests()
        val toTest = db?.intersect(enabledInTests) ?: enabledInTests - excludeSettings
        toTest.forEach { dbSettings ->
            try {
                withDb(dbSettings, statement)
            } catch (e: Exception) {
                throw AssertionError("Failed on ${dbSettings.name}", e)
            }
        }
    }

    fun withTables (excludeSettings: List<TestDB>, vararg tables: Table, statement: Transaction.(TestDB) -> Unit) {
        (TestDB.enabledInTests() - excludeSettings).forEach { testDB ->
            withDb(testDB) {
                addLogger(StdOutSqlLogger)
                SchemaUtils.create(*tables)
                try {
                    statement(testDB)
                    commit() // Need commit to persist data before drop tables
                } finally {
                    SchemaUtils.drop(*tables)
                    commit()
                }
            }
        }
    }

    fun withSchemas (excludeSettings: List<TestDB>, vararg schemas: Schema, statement: Transaction.() -> Unit) {
        (TestDB.enabledInTests() - excludeSettings).forEach { testDB ->
            withDb(testDB) {
                SchemaUtils.createSchema(*schemas)
                try {
                    statement()
                    commit() // Need commit to persist data before drop schemas
                } finally {
                    val cascade = false
                    SchemaUtils.dropSchema(*schemas, cascade = cascade)
                    commit()
                }
            }
        }
    }

    fun withTables (vararg tables: Table, statement: Transaction.(TestDB) -> Unit) = withTables(excludeSettings = emptyList(), tables = *tables, statement = statement)

    fun withSchemas (vararg schemas: Schema, statement: Transaction.() -> Unit) = withSchemas(excludeSettings = emptyList(), schemas = *schemas, statement = statement)
}

object Person: IntIdTable() {
    val username = varchar("name", 50)
    val age = integer("age")
    val password = varchar("password", 50)
}