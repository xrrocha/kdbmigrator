package testbed

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import arrow.core.traverse
import kdbmigrator.Failure
import kdbmigrator.MultipleFailure
import kdbmigrator.SqlTemplate
import kdbmigrator.attempt
import kdbmigrator.close
import kdbmigrator.closeBy
import kdbmigrator.partitionBy
import kotlinx.coroutines.runBlocking
import testbed.Target.ORIGIN
import java.sql.Connection

/* Compilable/Executable */
interface Executable<R> {
    fun execute(): Either<Failure, R>
}
typealias Compiled<R> = Either<Failure, Executable<R>>

interface Compilable<E, R> {
    fun compile(environment: E): Compiled<R>
}

/* DB Migration */
class DBMigrationEnv(
    val origin: Connection, val destination: Connection,
    val parameters: Map<String, Any?>, val batchSize: Int
)

enum class Timing {
    BEFORE, AFTER;

    override fun toString() = name.lowercase()
}

enum class Target {
    ORIGIN, DESTINATION;

    override fun toString() = name.lowercase()
}

class StatementBlock(val timing: Timing, val target: Target, sqlBlock: String) : Compilable<DBMigrationEnv, Int> {
    private val sqlTemplates = SqlTemplate.parse(sqlBlock)

    override fun compile(environment: DBMigrationEnv): Compiled<Int> {
        val connection = if (target == ORIGIN) environment.origin else environment.destination
        val (preparationErrors, statementPairs) =
            sqlTemplates.partitionBy { ps ->
                attempt({ "preparing statement $timing $target (${ps.sql})" }) {
                    ps.prepare(connection, environment.parameters) to ps.sql
                }
            }
        return if (preparationErrors.isNotEmpty()) {
            statementPairs.close { (preparedStatement, _) -> preparedStatement }
            MultipleFailure("preparing statements $timing $target", preparationErrors).left()
        } else {
            val executable = object : Executable<Int>, AutoCloseable {
                override fun execute(): Either<Failure, Int> =
                    statementPairs
                        .traverse { (preparedStatement, sql) ->
                            attempt({ "executing statement $timing $target:\n($sql)" }) {
                                preparedStatement.execute()
                            }
                        }
                        .map { it.size }

                override fun close() {
                    statementPairs.close { (preparedStatement, _) -> preparedStatement }
                }
            }
            executable.right()
        }
    }
}

interface Collator<E> {
    fun collate(environment: E, elements: List<Compilable<E, *>>): List<Pair<String, List<Compilable<E, *>>>>
}

class Interpreter<E>(private val collator: Collator<E>) {
    private val compilables = mutableListOf<Compilable<E, *>>()

    fun add(compilable: Compilable<E, *>) {
        compilables += compilable
    }

    fun interpret(environment: E): Either<Failure, Map<String, List<*>>> = runBlocking {
        either {
            val compilableToExecutable = compile(environment).bind()
            val executables = collator.collate(environment, compilables)
                .flatMap { (collationName, collatedCompilables) ->
                    collatedCompilables.map { compilable ->
                        val executable = compilableToExecutable[compilable]!!
                        collationName to executable
                    }
                }
            val resultPairs = executables.map { (collationName, executable) ->
                val executionResult = executable.execute().bind()
                collationName to executionResult
            }
            resultPairs
                .groupBy { (collationName, _) -> collationName }
                .mapValues { (_, resultPairs) -> resultPairs.map { (_, executionResult) -> executionResult } }
                .also {
                    executables.closeBy { (_, executable) -> executable }
                    compilables.close()
                }
        }
    }

    private fun compile(environment: E): Either<Failure, Map<Compilable<E, *>, Executable<*>>> {
        val (compilationErrors, executables) = compilables.partitionBy { it.compile(environment) }
        return if (compilationErrors.isEmpty()) {
            compilables.zip(executables).toMap().right()
        } else {
            executables.close()
            MultipleFailure("compiling ${compilables.size} elements", compilationErrors).left()
        }
    }
}

interface StepDefinitionState {
    fun beforeOrigin(sql: String): StepDefinitionState
    fun beforeDestination(sql: String): StepDefinitionState
    fun afterOrigin(sql: String): StepDefinitionState
    fun afterDestination(sql: String): StepDefinitionState
    fun step(select: String, insert: String, batchSize: Int? = null): StepDefinitionState
}