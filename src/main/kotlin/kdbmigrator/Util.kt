package kdbmigrator

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right

/* Failures */

data class Violation(val name: String, val message: String, val value: Any? = null, val cause: Throwable? = null)
class ValidationException(val violations: List<Violation>) : RuntimeException(message(violations)) {
    companion object {
        fun message(violations: List<Violation>) =
            violations.joinToString("\n") { it.message }
    }
}

open class Failure(val context: String, val cause: Throwable? = null) {
    constructor(cause: Throwable?, context: () -> String) : this(context(), cause)

    override fun toString() =
        "Error $context${if (cause == null) "" else " (${cause.message ?: cause})"}"
}

class MultipleFailure(context: String, val failures: List<Failure>, cause: Throwable? = null) :
    Failure(context, cause) {
    override fun toString() =
        "Error $context: ${if (cause == null) "" else " ${cause.message ?: cause} ($failures)"}"
}

/* Utility functions */

fun <T> Iterable<T>.toPair(): Pair<T, T> {
    val iterator = iterator()
    return iterator.next() to iterator.next()
}

fun ignoreFailure(action: () -> Unit) =
    try {
        action()
    } catch (_: Exception) {
        // TODO Log (finer) failure upon ignoring
    }

/* Either */
fun expect(condition: Boolean, message: () -> String): Either<Failure, Unit> =
    if (condition) Unit.right() else kdbmigrator.Failure(message()).left()

fun <R> attempt(context: String, action: () -> R): Either<Failure, R> =
    attempt({ context }, action)

fun <R> attempt(context: () -> String, action: () -> R): Either<Failure, R> =
    Either.catch { action() }.mapLeft { Failure(context(), it) }

fun <R> Either<Failure, R>.verify(condition: (R) -> Boolean, message: (R) -> String): Either<Failure, R> =
    flatMap {
        if (condition(it)) it.right()
        else Failure(message(it)).left()
    }

fun <L, R> Iterable<Either<L, R>>.rights(): List<R> =
    filterIsInstance<Right<R>>().map { it.value }

fun <L, R> Iterable<Either<L, R>>.lefts(): List<L> =
    filterIsInstance<Left<L>>().map { it.value }

fun <L, R> Iterable<Either<L, R>>.split(): Pair<List<L>, List<R>> {
    val (left, right) = this.partition { it.isLeft() }
    return Pair(left.map { (it as Left<L>).value }, right.map { (it as Right<R>).value })
}

// TODO Rename partitionBy to tryAndSplit or somesuch?
fun <L, R, T> Iterable<T>.partitionBy(action: (T) -> Either<L, R>): Pair<List<L>, List<R>> {
    val (left, right) = this.map(action).partition { it.isLeft() }
    return Pair(left.map { (it as Left<L>).value }, right.map { (it as Right<R>).value })
}

fun <R> Iterable<() -> R>.attempt(): Pair<List<Throwable>, List<R>> {
    val (left, right) = this
        .map {
            Either.catch(it)
        }
        .partition { it.isLeft() }
    return Pair(left.map { (it as Left<Throwable>).value }, right.map { (it as Right<R>).value })
}

/* AutoCloseable */
fun <T> Iterable<T>.close() = filterIsInstance<AutoCloseable>().map { ignoreFailure { it.close() } }

fun <T> Iterable<T>.close(extractor: (T) -> AutoCloseable) = map { ignoreFailure { extractor(it).close() } }
fun <T, R> Iterable<T>.closeBy(extractor: (T) -> R) =
    map(extractor).filterIsInstance<AutoCloseable>().map { ignoreFailure { it.close() } }
