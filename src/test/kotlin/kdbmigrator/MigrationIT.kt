package kdbmigrator

import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.fail

class MigrationIT {

    companion object : DBSupport()

    class H2(database: String) : Database {
        override val driver = org.h2.Driver()
        override val jdbcUrl = "jdbc:h2:mem:$database;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE"
    }

    private val testMigration = migration {

        prepare destination """
            SET REFERENTIAL_INTEGRITY FALSE;
            TRUNCATE TABLE emp;
            TRUNCATE TABLE dept;
        """

        wrapup destination """
            SET REFERENTIAL_INTEGRITY TRUE;
        """

        migration step "Load departments" runs {
            read using """
                SELECT deptno, dname, loc
                FROM   dept
            """

            write using """
                INSERT INTO dept(deptno, dname, loc, creation_date)
                VALUES(:deptno, :dname, :loc, :creation_date)
            """
        }

        migration step "Load employees" runs {

            batch size 4

            read using """
                SELECT empno, ename, job, mgr, hiredate, 
                       sal, comm, deptno
                FROM   emp
            """

            write using """
                INSERT INTO emp(empno, ename, job, mgr, hiredate, 
                                sal, comm, deptno, creation_date)
                VALUES(:empno, :ename, :job, :mgr, :hiredate, 
                       :sal, :comm, :deptno, :creation_date)
            """
        }
    }

    @Test
    fun `Migrates tables`() {
        listOf("origin", "destination").forEach { dbName ->
            createSchema(dbName)
            loadData(dbName)
        }
        testMigration
            .runWith(
                H2("origin").connect(credentials),
                H2("destination").connect(credentials),
                mapOf(
                    "creation_date" to LocalDate.now()
                )
            )
            .tap { results ->
                assertEquals(
                    setOf(
                        StepResult("Load employees", 14),
                        StepResult("Load departments", 4)

                    ),
                    results.toSet()
                )
            }
            .tapLeft { errors ->
                println("Errors:")
                errors.forEach(::println)
                fail()
            }
        val empNames =
            connect("destination")
                .prepareStatement("SELECT * FROM emp")
                .executeQuery()
                .readAll()
                .map { it["ENAME"]!! }
                .toSet()
        assertEquals(
            setOf(
                "ADAMS", "ALLEN", "BLAKE", "CLARK", "FORD", "JAMES", "JONES",
                "KING", "MARTIN", "MILLER", "SCOTT", "SMITH", "TURNER", "WARD"
            ),
            empNames
        )
    }
}