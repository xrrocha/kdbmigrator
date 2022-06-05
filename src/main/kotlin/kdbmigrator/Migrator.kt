package kdbmigrator

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import arrow.core.traverse
import kdbmigrator.Target.ORIGIN
import java.sql.Connection

/* DB Migration */
class DBEnvironment(
    val origin: Connection,
    val destination: Connection,
    val parameters: Map<String, Any?>,
    val batchSize: Int
)

class MigrationStep(
    selectSql: String,
    insertSql: String,
    batchSize: Int?,
    statementBlocks: List<StatementBlock>
) : Compilable<DBEnvironment, Int> {

    private val selectTemplate: SqlTemplate
    private val insertTemplate: SqlTemplate

    init {
        val batchSizeResult =
            if (batchSize == null || batchSize > 0) {
                batchSize.right()
            } else {
                Violation(
                    name = "batchSize",
                    message = "Batch size must be positive, got: $batchSize",
                    value = batchSize
                )
                    .left()
            }

        val sqlResults =
            listOf(
                "SELECT" to selectSql,
                "INSERT" to insertSql
            )
                .map { (verb, sql) ->
                    val sqlTemplate = SqlTemplate(sql)
                    if (sqlTemplate.sql.startsWith(verb, ignoreCase = true)) {
                        sqlTemplate.right()
                    } else {
                        Violation(
                            name = verb.lowercase() + "Sql",
                            message = "Statement is not $verb:\n${sqlTemplate.sql}",
                            value = sqlTemplate.sql
                        )
                            .left()
                    }
                }

        val errors = (sqlResults + batchSizeResult).lefts()
        if (errors.isNotEmpty()) {
            throw ValidationException(errors)
        }

        with(sqlResults.rights().iterator()) {
            selectTemplate = next()
            insertTemplate = next()
        }
    }

    override fun compile(environment: DBEnvironment): Compiled<Int> {
        TODO("Not yet implemented")
    }
}

class StatementBlock(val timing: Timing, val target: Target, sqlBlock: String) : Compilable<DBEnvironment, Int> {
    private val sqlTemplates = SqlTemplate.parse(sqlBlock)

    override fun compile(environment: DBEnvironment): Compiled<Int> {
        val connection = if (target == ORIGIN) environment.origin else environment.destination
        val (preparationErrors, statementPairs) =
            sqlTemplates.partitionBy { sqlTemplate ->
                sqlTemplate.prepare(connection, environment.parameters).map { it to sqlTemplate.sql }
            }
        return if (preparationErrors.isNotEmpty()) {
            statementPairs.close { (preparedStatement, _) -> preparedStatement }
            MultipleFailure("preparing statements $timing $target", preparationErrors).left()
        } else {
            val executable = object : Executable<Int>, AutoCloseable {
                override fun execute(): Either<Failure, Int> =
                    statementPairs
                        .traverse { (preparedStatement, sql) ->
                            attempt({ "executing statement $timing $target:\n$sql\n" }) {
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

interface StepDefinitionState {
    fun beforeOrigin(sql: String): StepDefinitionState
    fun beforeDestination(sql: String): StepDefinitionState
    fun afterOrigin(sql: String): StepDefinitionState
    fun afterDestination(sql: String): StepDefinitionState
    fun step(select: String, insert: String, batchSize: Int? = null): StepDefinitionState
}