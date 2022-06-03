package kdbmigrator

import java.sql.ResultSet
import java.util.Properties

open class DBSupport {
    private val driver = org.h2.Driver()
    private val schemaStatements = readStatements("schema.sql")
    private val dataStatements = readStatements("data.sql")

    val credentials = Properties().apply {
        setProperty("user", "sa")
        setProperty("password", "sa")
    }

    private fun jdbcUrl(name: String) = "jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE"
    private fun executeStatements(name: String, sqls: List<String>) =
        connect(name).use { connection ->
            for (sql in sqls) {
                connection.createStatement().execute(sql)
            }
        }

    fun connect(name: String) = driver.connect(jdbcUrl(name), credentials)!!
    fun createSchema(name: String) = executeStatements(name, schemaStatements)
    fun loadData(name: String) = executeStatements(name, dataStatements)

    fun ResultSet.readAll(): List<Map<String, Any?>> =
        generateSequence { }
            .takeWhile { next() }
            .map {
                (1..metaData.columnCount).associate {
                    metaData.getColumnLabel(it)!! to getObject(it)
                }
            }
            .toList()

    private fun readStatements(resourceName: String) =
        this::class.java.classLoader
            .getResourceAsStream(resourceName)!!
            .reader()
            .readText()
            .split(";")
            .map(String::trim)
            .filterNot(String::isEmpty)
}