package kdbmigrator

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.core.flatMap
import arrow.core.flatten
import arrow.core.traverse
import kdbmigrator.Target.DESTINATION
import kdbmigrator.Target.ORIGIN
import kdbmigrator.Timing.AFTER
import kdbmigrator.Timing.BEFORE
import java.io.File
import java.io.FileReader
import java.sql.Connection
import java.util.Properties

fun credentials(filename: String) =
    credentials(File(filename))

fun credentials(file: File) =
    Properties().apply { load(FileReader(file)) }

fun credentials(user: String, password: String) =
    Properties().apply {
        setProperty("user", user)
        setProperty("password", password)
    }

// TODO Validate w/Either
fun migration(commitAtEnd: Boolean = true, autoClose: Boolean = true, action: MigrationPlan.() -> Unit) =
    MigrationPlan(commitAtEnd, autoClose)
        .apply(action)
        .apply { validate() }

enum class Timing {
    BEFORE, AFTER;

    override fun toString() = name.lowercase()
}

enum class Target {
    ORIGIN, DESTINATION;

    override fun toString() = name.lowercase()
}

class MigrationPlan(private val commitAtEnd: Boolean, private val autoClose: Boolean) {

    class Runtime(
        val originConnection: Connection,
        val destinationConnection: Connection,
        val parameters: Map<String, Any?>,
        val batchSize: Int
    )

    interface ActionCompiler<R> {
        fun compile(runtime: Runtime): Either<List<Failure>, () -> Either<Failure, R>>
    }

    private val actionCompilers = mutableListOf<ActionCompiler<*>>()

    fun validate() {
        if (actionCompilers.filterIsInstance<StepCompiler>().isEmpty()) {
            throw IllegalStateException("No migration steps specified")
        }
    }

    class StatementCompiler(
        val timing: Timing,
        private val target: Target,
        private val sqlTemplate: SqlTemplate
    ) : ActionCompiler<Unit> {

        override fun compile(runtime: Runtime): Either<List<Failure>, () -> Either<Failure, Unit>> =
            sqlTemplate.prepare(connection(runtime), runtime.parameters)
                .map { statement ->
                    {
                        statement.use {
                            Either.catch {
                                // TODO Verify exclusion of SELECT statements
                                if (statement.metaData == null) {
                                    statement.execute()
                                }
                            }
                                // TODO Normalize parameterizedSql.sql
                                .mapLeft { Failure(it) { "executing '$timing' on $target (${sqlTemplate.sql})" } }
                        }
                    }
                }
                .mapLeft { listOf(it) }

        private fun connection(runtime: Runtime) =
            if (target == ORIGIN) runtime.originConnection
            else runtime.destinationConnection
    }

    class StepCompiler(private val step: Step) : ActionCompiler<StepResult> {

        override fun compile(runtime: Runtime): Either<List<Failure>, () -> Either<Failure, StepResult>> {

            val selectResult = step.select.prepare(runtime.originConnection, runtime.parameters)

            val insertResult = step.insert.prepare(
                runtime.destinationConnection,
                step.batchSize ?: runtime.batchSize
            )

            val failures = listOf(selectResult, insertResult).filterIsInstance<Left<Failure>>().map { it.value }
            if (failures.isNotEmpty()) {
                return Left(failures)
            }

            val selectStatement = selectResult.orNull()!!
            val insert = insertResult.orNull()!!

            return Right {
                // TODO selectStatement.executeQuery() can fail
                insert.populateFrom(selectStatement.executeQuery(), runtime.parameters)
                    .map { StepResult(step.name, it) }
            }
        }
    }

    interface OriginDestination {
        infix fun origin(sql: String)
        infix fun destination(sql: String)
    }

    val prepare = object : OriginDestination {
        override infix fun origin(sql: String) =
            addAction(BEFORE, ORIGIN, sql)

        override infix fun destination(sql: String) =
            addAction(BEFORE, DESTINATION, sql)
    }
    val wrapup = object : OriginDestination {
        override infix fun origin(sql: String) =
            addAction(AFTER, ORIGIN, sql)

        override infix fun destination(sql: String) =
            addAction(AFTER, DESTINATION, sql)
    }

    internal fun addAction(timing: Timing, target: Target, sql: String) {
        // TODO Parse SQL quoted strings
        actionCompilers +=
            sql
                .split(";")
                .map(String::trim)
                .filterNot { it.isEmpty() || it == ";" }
                .map(::SqlTemplate)
                .map { StatementCompiler(timing, target, it) }

    }

    interface Migration {
        infix fun step(name: String): String = name
    }

    val migration = object : Migration {}

    // TODO Validate w/Either
    infix fun String.runs(action: Step.() -> Unit) {
        actionCompilers += StepCompiler(
            Step(this)
                .apply(action)
                .apply { validate() })
    }

    internal fun ignoreFailure(action: () -> Unit) =
        try {
            action()
        } catch (_: Exception) {
        }

    fun runWith(
        originConnection: Connection,
        destinationConnection: Connection,
        parameters: Map<String, Any?> = emptyMap(),
        batchSize: Int = 1024
    ): Either<List<Failure>, List<StepResult>> =
        try {
            runWith(Runtime(originConnection, destinationConnection, parameters, batchSize))
                .flatMap { stepResults ->
                    val errors =
                        if (!commitAtEnd) {
                            emptyList()
                        } else {
                            listOf(
                                "origin" to { originConnection.commit() },
                                "destination" to { destinationConnection.commit() }
                            )
                                .partitionBy { (context, action) ->
                                    Either.catch { action() }
                                        .mapLeft { Failure(it) { "committing on $context database" } }
                                }
                                .first // List<Failure>
                        }
                    if (errors.isEmpty()) {
                        Right(stepResults)
                    } else {
                        Left(errors)
                    }
                }
                .tapLeft {
                    if (commitAtEnd) {
                        ignoreFailure { originConnection.rollback() }
                        ignoreFailure { destinationConnection.rollback() }
                    }
                }
        } finally {
            if (autoClose) {
                ignoreFailure { originConnection.close() }
                ignoreFailure { destinationConnection.close() }
            }
        }

    private fun runWith(runtime: Runtime): Either<List<Failure>, List<StepResult>> {

        val (stepPartition, statementPartition) = actionCompilers.partition { it is StepCompiler }

        val (stepFailures, stepActions) =
            stepPartition.partitionBy { (it as StepCompiler).compile(runtime) }

        val statementPreparers = statementPartition.map { it as StatementCompiler }
        val (beforePreparers, afterPreparers) = statementPreparers.partition { it.timing == BEFORE }
        val (beforeFailures, beforeActions) = beforePreparers.partitionBy { it.compile(runtime) }
        val (afterFailures, afterActions) = afterPreparers.partitionBy { it.compile(runtime) }

        val failures =
            listOf(stepFailures, beforeFailures, afterFailures).map { it.flatten() }.flatten()
        if (failures.isNotEmpty()) {
            return Left(failures)
        }

        fun <T> List<() -> Either<Failure, T>>.execute(): Either<Failure, List<T>> =
            traverse { it.invoke() }

        return beforeActions.execute()
            .flatMap { stepActions.execute() }
            .flatMap { migrationResults -> afterActions.execute().map { migrationResults } }
            .mapLeft { listOf(it) } // TODO Retain migration step counts on AFTER failure
    }
}

// TODO Add before/after to migration step
class Step(val name: String) {

    lateinit var select: SqlTemplate
    lateinit var insert: SqlTemplate

    // TODO Hide batchSize get/set
    var batchSize: Int? = null

    fun validate() {
        val messages = mutableListOf<String>()
        if (!this::select.isInitialized) {
            messages += "No select statement specified"
        }
        if (!this::insert.isInitialized) {
            messages += "No insert statement specified"
        }
        if (batchSize != null && batchSize!! <= 0) {
            messages += "Invalid batch size: $batchSize"
        }
        if (messages.isNotEmpty()) {
            throw IllegalStateException(messages.joinToString(". "))
        }
    }

    interface Using {
        infix fun using(sql: String)
    }

    val read = object : Using {
        override fun using(sql: String) {
            select = SqlTemplate(sql)
        }
    }
    val write = object : Using {
        override fun using(sql: String) {
            insert = SqlTemplate(sql)
        }
    }

    interface Batch {
        infix fun size(value: Int)
    }

    val batch = object : Batch {
        override fun size(value: Int) {
            batchSize = value
        }
    }
}

data class StepResult(val name: String, val count: Int) {
    override fun toString() = "$name: $count rows migrated"
}
