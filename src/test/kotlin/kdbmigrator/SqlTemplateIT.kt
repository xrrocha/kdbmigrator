package kdbmigrator

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.assertThrows
import java.sql.Types
import kotlin.test.Test
import kotlin.test.assertEquals

class SqlTemplateIT {

    companion object : DBSupport()

    @Test
    fun `Prepares SELECT statement correctly`() {
        val sqlTemplate = SqlTemplate(
            """
           SELECT ename
           FROM   emp
           WHERE  deptno = :deptno
        """
        )
        createSchema("test")
        loadData("test")
        val results = sqlTemplate
            .prepare(connect("test"), mapOf("deptno" to 10))
            .executeQuery()
            .readAll()
            .map { it["ENAME"] as String }
            .toSet()
        assertEquals(setOf("KING", "CLARK", "MILLER"), results)
    }

    @Test
    fun `Fails on missing parameter`() {
        val sqlTemplate = SqlTemplate(
            """
           SELECT ename
           FROM   emp
           WHERE  deptno = :deptno
        """
        )
        createSchema("test")
        loadData("test")
        assertThrows<IllegalArgumentException> {
            sqlTemplate.prepare(connect("test"), emptyMap())
        }
    }

    @Test
    @Disabled
    fun `Fails on parameter count mismatch`() {
        val sqlTemplate = SqlTemplate(
            """
           SELECT ':name' AS name
           FROM   emp
           WHERE  deptno = :deptno
        """
        )
        createSchema("test")
        loadData("test")
        assertThrows<IllegalStateException> {
            sqlTemplate.prepare(connect("test"), mapOf("deptno" to 10))
        }
    }

    @Test
    fun `Fails on incomplete SELECT parameters`() {
    }

    @Test
    fun `Fails on parameter type mismatch`() {
    }

    @Test
    fun `Adds insert parameters`() {
    }

    @Test
    fun `Inserts row count greater than batch size`() {
    }

    @Test
    fun `Populates insert from result set`() {
    }

    @Test
    fun `Determines parameter types`() {
        val sqlTemplate = SqlTemplate(
            """
           SELECT empno, ename, sal
           FROM   emp
           WHERE  deptno = :deptno
              OR  sal > :min_sal
        """
        )
        createSchema("test")
        loadData("test")
        sqlTemplate.prepare(
            connect("test"),
            mapOf(
                "deptno" to 10,
                "min_sal" to 7000.0
            )
        )
        assertEquals(
            listOf(Types.INTEGER, Types.NUMERIC),
            sqlTemplate.parameterTypes
        )
    }
}