package kdbmigrator

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right

/* Compilable/Executable */
interface Executable<R> {
    fun execute(): Either<Failure, R>
}
typealias Compiled<R> = Either<Failure, Executable<R>>

interface Compilable<E, R> {
    fun compile(environment: E): Compiled<R>
}

interface Collator<E> {
    fun collate(environment: E, elements: List<Compilable<E, *>>): List<Pair<String, List<Compilable<E, *>>>>
}

class Interpreter<E>(private val collator: Collator<E>) {
    private val compilables = mutableListOf<Compilable<E, *>>()

    fun add(compilable: Compilable<E, *>) {
        compilables += compilable
    }

    fun interpret(environment: E): Either<Failure, Map<String, List<*>>> = either.eager {
        val compilableToExecutable = compile(environment).bind()
        val executables = collator.collate(environment, compilables)
            .flatMap { (collationName, collatedCompilables) ->
                collatedCompilables.map { compilable ->
                    val executable = compilableToExecutable[compilable]!!
                    collationName to executable
                }
            }
        val collatedResults = executables.map { (collationName, executable) ->
            val executionResult = executable.execute().bind()
            collationName to executionResult
        }
        collatedResults
            .groupBy { (collationName, _) -> collationName }
            .mapValues { (_, resultPairs) -> resultPairs.map { (_, executionResult) -> executionResult } }
            .also {
                executables.closeBy { (_, executable) -> executable }
                compilables.close()
            }
    }


    private fun compile(environment: E): Either<Failure, Map<Compilable<E, *>, Executable<*>>> {
        val (compilationErrors, executables) = compilables.partitionBy { it.compile(environment) }
        return if (compilationErrors.isEmpty()) {
            compilables.zip(executables).toMap().right()
        } else {
            executables.close()
            MultipleFailure("compiling ${compilables.size} elements", compilationErrors).left()
        }
    }
}
