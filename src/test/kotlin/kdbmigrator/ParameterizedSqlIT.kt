package kdbmigrator

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.assertThrows
import java.sql.Types
import kotlin.test.Test
import kotlin.test.assertEquals

class ParameterizedSqlIT {

    companion object : DBSupport()

    @Test
    fun `Prepares SELECT statement correctly`() {
        val parameterizedSql = ParameterizedSql(
            """
           SELECT ename
           FROM   emp
           WHERE  deptno = :deptno
        """
        )
        createSchema("test")
        loadData("test")
        val results = parameterizedSql
            .prepare(connect("test"), mapOf("deptno" to 10))
            .executeQuery()
            .readAll()
            .map { it["ENAME"] as String }
            .toSet()
        assertEquals(setOf("KING", "CLARK", "MILLER"), results)
    }

    @Test
    fun `Fails on missing parameter`() {
        val parameterizedSql = ParameterizedSql(
            """
           SELECT ename
           FROM   emp
           WHERE  deptno = :deptno
        """
        )
        createSchema("test")
        loadData("test")
        assertThrows<IllegalArgumentException> {
            parameterizedSql.prepare(connect("test"), emptyMap())
        }
    }

    @Test
    @Disabled
    fun `Fails on parameter count mismatch`() {
        val parameterizedSql = ParameterizedSql(
            """
           SELECT ':name' AS name
           FROM   emp
           WHERE  deptno = :deptno
        """
        )
        createSchema("test")
        loadData("test")
        assertThrows<IllegalStateException> {
            parameterizedSql.prepare(connect("test"), mapOf("deptno" to 10))
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
        val parameterizedSql = ParameterizedSql(
            """
           SELECT empno, ename, sal
           FROM   emp
           WHERE  deptno = :deptno
              OR  sal > :min_sal
        """
        )
        createSchema("test")
        loadData("test")
        parameterizedSql.prepare(
            connect("test"),
            mapOf(
                "deptno" to 10,
                "min_sal" to 7000.0
            )
        )
        assertEquals(
            listOf(Types.INTEGER, Types.NUMERIC),
            parameterizedSql.parameterTypes
        )
    }
}