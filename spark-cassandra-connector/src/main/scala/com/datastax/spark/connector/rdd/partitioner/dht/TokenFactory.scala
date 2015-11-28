package com.datastax.spark.connector.rdd.partitioner.dht

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.cassandra.dht._

trait TokenFactory[V, T <: Token[V]] {
  def minToken: T
  def maxToken: T

  /** Total token count in a ring. */
  def totalTokenCount: BigInt

  /** Number of tokens in a range from `token1` to `token2`.
    * If `token2 < token1`, then range wraps around. */
  def distance(token1: T, token2: T): BigInt

  /** Fraction of the ring in a range from `token1` to `token2`.
    * If `token2 < token1`, then range wraps around.
    * Returns 1.0 for a full ring range, 0.0 for an empty range. */
   def ringFraction(token1: T, token2: T): Double =
    distance(token1, token2).toDouble / totalTokenCount.toDouble

  /** Creates a token from its string representation */
  def tokenFromString(string: String): T

  /** Converts a token to its string representation */
  def tokenToString(token: T): String

  var startToken: Option[T] = None
  var endToken: Option[T] = None

  def setStartToken(token: String): Unit = {
    startToken = Some(tokenFromString(token))
  }
  def setEndToken(token: String): Unit = {
    endToken = Some(tokenFromString(token))
  }
}

object TokenFactory {

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  implicit object Murmur3TokenFactory extends TokenFactory[Long, LongToken] {
    override val minToken = LongToken(Long.MinValue)
    override val maxToken = LongToken(Long.MaxValue)
    override val totalTokenCount = BigInt(maxToken.value) - BigInt(minToken.value)
    override def tokenFromString(string: String) = LongToken(string.toLong)
    override def tokenToString(token: LongToken) = token.value.toString

    override def setStartToken(token: String): Unit = {
      val bToken: ByteBuffer = ByteBuffer.wrap(token.getBytes())
      val partitioner = new Murmur3Partitioner()
      startToken = Some(LongToken(partitioner.getToken(bToken).getTokenValue.asInstanceOf[Long]))
    }

    override def setEndToken(token: String): Unit = {
      val bToken: ByteBuffer = ByteBuffer.wrap(token.getBytes())
      val partitioner = new Murmur3Partitioner()
      endToken = Some(LongToken(partitioner.getToken(bToken).getTokenValue.asInstanceOf[Long]))
    }
    override def distance(token1: LongToken, token2: LongToken): BigInt = {
      val left = token1.value
      val right = token2.value
      if (right > left) BigInt(right) - BigInt(left)
      else BigInt(right) - BigInt(left) + totalTokenCount
    }
  }

  implicit object RandomPartitionerTokenFactory extends TokenFactory[BigInt, BigIntToken] {
    override val minToken = BigIntToken(-1)
    override val maxToken = BigIntToken(BigInt(2).pow(127))
    override val totalTokenCount = maxToken.value - minToken.value
    override def tokenFromString(string: String) = BigIntToken(BigInt(string))
    override def tokenToString(token: BigIntToken) = token.value.toString()

    override def distance(token1: BigIntToken, token2: BigIntToken) = {
      val left = token1.value
      val right = token2.value
      if (right > left) right - left
      else right - left + totalTokenCount
    }
  }

  def forCassandraPartitioner(partitionerClassName: String): TokenFactory[V, T] = {
    val partitioner =
      partitionerClassName match {
        case "org.apache.cassandra.dht.Murmur3Partitioner" => Murmur3TokenFactory
        case "org.apache.cassandra.dht.RandomPartitioner" => RandomPartitionerTokenFactory
        case _ => throw new IllegalArgumentException(s"Unsupported partitioner: $partitionerClassName")
      }
    partitioner.asInstanceOf[TokenFactory[V, T]]
  }
}




