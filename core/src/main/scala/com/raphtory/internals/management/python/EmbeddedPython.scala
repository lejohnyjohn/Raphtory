package com.raphtory.internals.management.python

import cats.Id
import com.raphtory.internals.management.PyRef
import com.raphtory.internals.management.PythonEncoder

trait EmbeddedPython[IO[_]] {

  def invoke(ref: PyRef, methodName: String, args: Vector[Object] = Vector.empty): IO[Object]

  def eval[T](expr: String)(implicit PE: PythonEncoder[T]): IO[T]

  def run(script: String): IO[Unit]

  def set(name: String, obj: Any): IO[Unit]
}

object EmbeddedPython {
  private var globalInterpreter: Option[EmbeddedPython[Id]] = None
  def injectInterpreter(interpreter: EmbeddedPython[Id]) =
    globalInterpreter = Some(interpreter)
  private val interpreters       = ThreadLocal.withInitial[EmbeddedPython[Id]](() => UnsafeEmbeddedPython.apply())
  def global: EmbeddedPython[Id] = globalInterpreter.getOrElse(interpreters.get())
}
