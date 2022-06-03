package kdbmigrator

import java.sql.Connection
import java.sql.Driver
import java.util.Properties

interface Database {

    val driver: Driver
    val jdbcUrl: String

    fun connect(filename: String, autoCommit: Boolean = false): Connection =
        connect(credentials(filename), autoCommit)

    fun connect(properties: Properties, autoCommit: Boolean = false): Connection =
        driver.connect(jdbcUrl, properties).apply {
            this.autoCommit = autoCommit
        }

    fun connect(user: String, password: String, autoCommit: Boolean = false): Connection =
        connect(
            Properties().apply {
                setProperty("user", user)
                setProperty("password", password)
            },
            autoCommit
        )

    operator fun invoke(props: () -> Properties): () -> Connection =
        { connect(props()) }

    operator fun invoke(user: String, password: String): () -> Connection =
        { connect(user, password) }
}

class Oracle(
    host: String,
    service: String,
    port: Int = 1521
) : Database {
    override val driver = oracle.jdbc.OracleDriver()
    override val jdbcUrl = "jdbc:oracle:thin:@//$host:$port/$service"
}

class Postgres(
    host: String,
    database: String,
    port: Int = 5432
) : Database {
    override val driver = org.postgresql.Driver()
    override val jdbcUrl = "jdbc:postgresql://$host:$port/$database"
}
