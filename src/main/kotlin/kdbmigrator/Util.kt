package kdbmigrator

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.core.left
import arrow.core.right

/* Failures */
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
fun expect(condition: Boolean, message: () -> String): Either<Failure, Unit> =
    if (condition) Unit.right() else kdbmigrator.Failure(message()).left()

fun <R> attempt(context: () -> String, action: () -> R): Either<Failure, R> =
    Either.catch { action() }.mapLeft { Failure(context(), it) }

fun <L, R, T> Iterable<T>.partitionBy(action: (T) -> Either<L, R>): Pair<List<L>, List<R>> {
    val (left, right) = this.map(action).partition { it.isLeft() }
    return Pair(left.map { (it as Left<L>).value }, right.map { (it as Right<R>).value })
}

fun <T> List<T>.close() = filterIsInstance<AutoCloseable>().map { ignoreFailure { it.close() } }

fun <T> List<T>.close(extractor: (T) -> AutoCloseable) = map { ignoreFailure { extractor(it).close() } }
fun <T, R> List<T>.closeBy(extractor: (T) -> R) =
    map(extractor).filterIsInstance<AutoCloseable>().map { ignoreFailure { it.close() } }

fun ignoreFailure(action: () -> Unit) =
    try {
        action()
    } catch (_: Exception) {
        // TODO Log (finer) failure upon ignoring
    }
