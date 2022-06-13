package com.raphtory.internals.management.id

import cats.effect.Resource
import cats.effect.Sync
import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.retry.RetryNTimes
import org.slf4j.LoggerFactory;

private[raphtory] class ZookeeperIDManager(
    zookeeperAddress: String,
    atomicPath: String,
    client: CuratorFramework
) extends IDManager {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val atomicInt: DistributedAtomicInteger =
    new DistributedAtomicInteger(client, atomicPath, new RetryNTimes(10, 500), null);

  def getNextAvailableID(): Option[Int] = {
    val incremented = atomicInt.increment()

    if (incremented.succeeded()) {
      val id = incremented.preValue()

      logger.trace(s"Zookeeper $zookeeperAddress: Atomic integer pre value at '$id'.")

      Some(id)
    }
    else {
      logger.error(s"Zookeeper $zookeeperAddress: Failed to increment atomic integer.")

      None
    }
  }

  def resetID(): Unit = {
    logger.debug(s"Zookeeper $zookeeperAddress: Atomic integer requested for reset.")

    logger.debug(
            s"Zookeeper $zookeeperAddress: Atomic integer value now at '${atomicInt.get().preValue()}'."
    )

    atomicInt.forceSet(0)

    logger.debug(
            s"Zookeeper $zookeeperAddress: Atomic integer value now at '${atomicInt.get().postValue()}'."
    )
  }

}

object ZookeeperIDManager {

  def apply[IO[_]: Sync](
      zookeeperAddress: String,
      atomicPath: String
  ): Resource[IO, ZookeeperIDManager] =
    Resource
      .fromAutoCloseable(Sync[IO].delay {
        CuratorFrameworkFactory
          .builder()
          .connectString(zookeeperAddress)
          .retryPolicy(new ExponentialBackoffRetry(1000, 3))
          .build();
      })
      .map(new ZookeeperIDManager(zookeeperAddress, atomicPath, _))
}
