package kdbmigrator

import kotlin.test.Test
import kotlin.test.assertEquals

class SqlTemplateTest {
    @Test
    fun `Matches proper parameter names`() {
        listOf(
            ":param", ":_param", ":param0", ":param_1",
            ":_param2", ":_param_34", ":param_5_",
            ":param_6_a", ":param_7_a_", ":param_8_z_0"
        )
            .all(SqlTemplate.ParameterReference::matches)
    }

    @Test
    fun `Doesn't match improper parameter`() {
        listOf(
            "", ":", "param", ":1param", ""
        )
            .none(SqlTemplate.ParameterReference::matches)
    }

    @Test
    fun `Parses non-parameterized sql properly`() {
        val sqlTemplate = SqlTemplate("""
            SELECT  empno, ename, sal
            FROM    emp
        """)
        assertEquals("""
            SELECT  empno, ename, sal
            FROM    emp
        """, sqlTemplate.parameterizedSql)
        assertEquals(emptyList(), sqlTemplate.parameterNames)
    }

    @Test
    fun `Parses parameterized sql properly`() {
        val sqlTemplate = SqlTemplate("""
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
        """, sqlTemplate.parameterizedSql)
        assertEquals(listOf("deptno", "min_sal", "deptno"), sqlTemplate.parameterNames)
    }
}