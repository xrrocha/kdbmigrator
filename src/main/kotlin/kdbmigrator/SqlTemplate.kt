package kdbmigrator

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.traverse
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

class SqlTemplate(val sql: String) {

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
    fun doPrepare(connection: Connection, parameters: Map<String, Any?> = emptyMap()):
            Either<Failure, PreparedStatement> = runBlocking {
        either {

            val preparedStatement = connection.prepareStatement(parameterizedSql)

            expect(preparedStatement.parameterMetaData.parameterCount == parameterNames.size) {
                "Statement parameter count mismatch, " +
                        "expected: ${preparedStatement.parameterMetaData.parameterCount}, " +
                        "got: ${parameterNames.size}"
            }
                .bind()

            expect(preparedStatement.metaData == null || parameterNames.all(parameters::containsKey)) {
                "Statement parameter count mismatch, " +
                        "expected: ${preparedStatement.parameterMetaData.parameterCount}, " +
                        "got: ${parameterNames.size}"
            }
                .bind()

            parameterTypes =
                (1..preparedStatement.parameterMetaData.parameterCount)
                    .traverse {
                        attempt({ "getting parameter type #$it" }) {
                            preparedStatement.parameterMetaData.getParameterType(
                                it
                            )
                        }
                    }
                    .bind()

            setParameters(preparedStatement, parameters)

            preparedStatement
        }
    }

    fun prepare(connection: Connection, parameters: Map<String, Any?> = emptyMap()): PreparedStatement {
        return connection.prepareStatement(parameterizedSql).apply {
            if (parameterMetaData.parameterCount != parameterNames.size) {
                throw IllegalStateException(
                    "Statement parameter count mismatch, " +
                            "expected: ${parameterMetaData.parameterCount}, got: ${parameterNames.size}"
                )
            }
            if (metaData != null && !parameterNames.all(parameters::containsKey)) { // SELECT
                throw IllegalArgumentException("Missing statement parameters: ${parameterNames.minus(parameters.keys)}")
            }
            parameterTypes = (1..parameterMetaData.parameterCount)
                .map(parameterMetaData::getParameterType)
                .toList()
            setParameters(this, parameters)
        }
    }

    internal fun setParameters(statement: PreparedStatement, parameters: Map<String, Any?>) {
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

    fun prepare(connection: Connection, batchSize: Int): Insert =
        Insert(connection, batchSize)

    inner class Insert(connection: Connection, private val batchSize: Int) : AutoCloseable {

        private var count = 0
        private val statement = prepare(connection)

        fun populateFrom(resultSet: ResultSet, parameters: Map<String, Any?>): Int {
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

        fun add(parameters: Map<String, Any?>) {
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