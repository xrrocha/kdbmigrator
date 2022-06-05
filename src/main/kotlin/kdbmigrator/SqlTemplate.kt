package kdbmigrator

import arrow.core.Either
import arrow.core.continuations.either
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

class SqlTemplate(sql: String) {

    val sql = sql.trimIndent()

    companion object {
        internal val ParameterReference = """:[_\p{IsLatin}][_\p{IsLatin}\d]+""".toRegex()
        fun parse(sqlBlock: String): List<SqlTemplate> =
            sqlBlock
                .split(";")
                .map(String::trim)
                .filterNot { it.isEmpty() || it == ";" }
                .map(::SqlTemplate)

    }

    internal val parameterizedSql: String =
        ParameterReference.replace(sql, "?")
    internal val parameterNames: List<String> =
        ParameterReference
            .findAll(sql)
            .map { it.value.substring(1) }
            .toList()

    internal lateinit var parameterTypes: List<Int>

    fun prepare(connection: Connection, parameters: Map<String, Any?>)
            : Either<Failure, PreparedStatement> = runBlocking {
        either {

            val preparedStatement =
                attempt({ "preparing SQL statement:\n$sql\n" }) {
                    connection.prepareStatement(parameterizedSql)
                }
                    .verify({ it.parameterMetaData.parameterCount == parameterNames.size }) {
                        "Statement parameter count mismatch, " +
                                "expected: ${it.parameterMetaData.parameterCount}, " +
                                "got: ${parameterNames.size}"
                    }
                    .verify({ it.metaData == null || parameterNames.all(parameters::containsKey) }) {
                        "Missing statement parameters: ${parameterNames.minus(parameters.keys)}"
                    }
                    .bind()

            parameterTypes =
                attempt({ "getting parameter types" }) {
                    with(preparedStatement.parameterMetaData) {
                        (1..parameterCount).map(::getParameterType)
                    }
                }
                    .bind()

            attempt({ "setting ${parameters.size} parameters" }) {
                setParameters(preparedStatement, parameters)
            }
                .bind()

            preparedStatement
        }
    }

    private fun setParameters(statement: PreparedStatement, parameters: Map<String, Any?>) {
        statement.clearParameters()
        (1..parameterNames.size).forEach { i ->
            val parameterName = parameterNames[i - 1]
            val parameterValue = parameters[parameterName]
            if (parameterValue != null) {
                statement.setObject(i, parameterValue)
            } else {
                statement.setNull(i, parameterTypes[i - 1])
            }
        }
    }

    fun prepare(connection: Connection, batchSize: Int): Either<Failure, Insert> =
        prepare(connection, emptyMap()).map { Insert(it, batchSize) }

    inner class Insert(private val statement: PreparedStatement, private val batchSize: Int) : AutoCloseable {

        private var count = 0

        fun populateFrom(resultSet: ResultSet, parameters: Map<String, Any?>): Either<Failure, Int> =
            attempt("populating insert from result set:\n$sql\n") { doPopulateFrom(resultSet, parameters) }

        private fun doPopulateFrom(resultSet: ResultSet, parameters: Map<String, Any?>): Int {
            use {
                resultSet.use { resultSet ->
                    while (resultSet.next()) {
                        add(HashMap(parameters).apply {
                            putAll((1..resultSet.metaData.columnCount).associate { index ->
                                resultSet.metaData.getColumnLabel(index).lowercase() to resultSet.getObject(index)
                            })
                        })
                    }
                }
                if (count % batchSize != 0) {
                    statement.executeBatch()
                }
            }
            return count
        }

        private fun add(parameters: Map<String, Any?>) {
            setParameters(statement, parameters)
            statement.addBatch()

            count += 1
            if (count % batchSize == 0) {
                statement.executeBatch()
            }
        }

        override fun close() {
            statement.close()
        }
    }
}
