package kdbmigrator

import kotlin.test.Test
import kotlin.test.assertEquals

class ParameterizedSqlTest {
    @Test
    fun `Matches proper parameter names`() {
        listOf(
            ":param", ":_param", ":param0", ":param_1",
            ":_param2", ":_param_34", ":param_5_",
            ":param_6_a", ":param_7_a_", ":param_8_z_0"
        )
            .all(ParameterizedSql.ParameterReference::matches)
    }

    @Test
    fun `Doesn't match improper parameter`() {
        listOf(
            "", ":", "param", ":1param", ""
        )
            .none(ParameterizedSql.ParameterReference::matches)
    }

    @Test
    fun `Parses non-parameterized sql properly`() {
        val parameterizedSql = ParameterizedSql("""
            SELECT  empno, ename, sal
            FROM    emp
        """)
        assertEquals("""
            SELECT  empno, ename, sal
            FROM    emp
        """, parameterizedSql.parameterizedSql)
        assertEquals(emptyList(), parameterizedSql.parameterNames)
    }

    @Test
    fun `Parses parameterized sql properly`() {
        val parameterizedSql = ParameterizedSql("""
            SELECT  empno, ename, sal
            FROM    emp
            WHERE   deptno = :deptno
               OR   (sal >= :min_sal AND deptno <> :deptno)
        """)
        assertEquals("""
            SELECT  empno, ename, sal
            FROM    emp
            WHERE   deptno = ?
               OR   (sal >= ? AND deptno <> ?)
        """, parameterizedSql.parameterizedSql)
        assertEquals(listOf("deptno", "min_sal", "deptno"), parameterizedSql.parameterNames)
    }
}