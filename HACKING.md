# Highlevel description

from https://github.com/tensorflow/models/tree/master/swivel#swivel-in-tensorflow

Compute the co-occurrence statistics from a corpus; that is, determine how often a word c appears the context (e.g., "within ten words") of a focus word f. This results in a sparse co-occurrence matrix whose rows represent the focus words, and whose columns represent the context words. Each cell value is the number of times the focus and context words were observed together.

Note that the resulting co-occurrence matrix is very sparse (i.e., contains many zeros) since most words won't have been observed in the context of other words. In the case of very rare words, it seems reasonable to assume that you just haven't sampled enough data to spot their co-occurrence yet. On the other hand, if we've failed to observed to common words co-occuring, it seems likely that they are anti-correlated.

## Input

*tokens.tsv* (line: all tokens in a file)

  --input <filename>
  --output_dir <directory>
  --shard_size <int>
      Specifies the shard size; default 4096.
  --min_count <int>
      The minimum number of times a word should appear to be included in the
      generated vocabulary; default 5.  (Ignored if --vocab is used.)
  --max_vocab <int>
      The maximum vocabulary size to generate from the input corpus; default
      102,400.  (Ignored if --vocab is used.)
      Admit at most n words into the vocabulary.
  --vocab <filename>
      Use the specified unigram vocabulary instead of generating
      it from the corpus. The file should contain one word per line.
  --window_size <int>
      Specifies the window size for computing co-occurrence stats;
      default 10.

## Output

 - *row_vocab.txt, col_vocab.txt*

    The row and column vocabulary files.  Each file should contain one token per
    line; these will be used to generate a tab-separate file containing the
    trained embeddings.

 - *row_sums.txt,  col_sum.txt*

    The matrix row and column marginal sums.  Each file should contain one
    decimal floating point number per line which corresponds to the marginal
    count of the matrix for that row or column.

 - *shard-*.pb* as 'shard-%03d-%03d.pb' % (row, col)

   sub-matrix shards, stored as TFRecords
   Each shard is serialzed tf.Example protocol buffer with:
      global_row: the global row indicies contained in the shard
      global_col: the global column indicies contained in the shard
      sparse_local_row, sparse_local_col, sparse_value: three parallel arrays
      that are a sparse representation of the submatrix counts.


# Preparing the data for training

Once you've downloaded the corpus (e.g., to /tmp/wiki.txt), run prep.py to prepare the data for training:

./prep.py --output_dir /tmp/swivel_data --input /tmp/wiki.txt

prep.py computes a vocabulary and token co-occurrence statistics for the corpus. It then outputs the information into a format that can be digested by the TensorFlow trainer.

By default, prep.py will make one pass through the text file to compute a "vocabulary" of the most frequent words, and then a second pass to compute the co-occurrence statistics.


# Impl

PB using https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector

Need to define schema.


```scala
val df: DataFrame = spark.createDataFrame(rdd, schema)
df.write.format("tfrecords").save(path)

```

Highlevel overview

```python
  if FLAGS.vocab:
    vocab = open(FLAGS.vocab, 'r')
  else:
    vocab = create_vocabulary(lines)

  shardfiles, sums = compute_coocs(lines, vocab)  # 100 LOC
    # num_shards = len(vocab) / FLAGS.shard_size
    # each shard is square martix
  write_shards(vocab, shardfiles)                  # 150 LOC
    # Sort ansd merge co-occurrences for the same pairs. (reduce)
    # Convert to a TF Example proto.

  # Now write the marginals.  They're symmetric for this application.
  write_vocab_and_sums(vocab, sums, 'row_vocab.txt', 'row_sums.txt')
  write_vocab_and_sums(vocab, sums, 'col_vocab.txt', 'col_sums.txt')
```



# Plan
 - [x] run `prep.py`
 - [x] trace `prep.py` to understand algorithm
 - [x] profile `prep.py`:
   * who is slow compute_coocs or write_shards? CPU vs IO?
   * create_vocabulary() / compute_coocs() / write_shards() ?
   compute_coocs is slowest
 - [x] fastprep build & run on same tonkens.tsv
 - [x] Docker for fastprep
   https://github.com/tensorflow/models/pull/1108#issuecomment-298866790

 - write validation script
    * plot `matrix -> shards` heatmap
    * Byte-to-byte test on final .pb shards

 - pySpark using prep.py#compute_coocs code?

 Spark
 - [x] build vocabulary
   * output `{row, col}_vocab.txt` vocabularry
   * build and .brodcast() vocabulary/inverse
 - [x] generate co-occurrences
 - [ ] shard partitioning  `(i mod shard_size, j)`
 - outup *.pb per partion using
   https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector
 - output `{row, col}_sums.txt`
 - validation: dicts, sums, merged&sorted .pb
 - run on real cluster
 - add proper CLI: .sh wrapper + print usage
 - add CI
 - prepare date for large-scale testing
 - test at scale
 - experiment with DataFrame impl
   https://github.com/sryza/aas/blob/master/ch08-geotime/src/main/scala/com/cloudera/datascience/geotime/RunGeoTime.scala#L83



------------

Working though some example of co-occurrences
```
window_size = 3

# a b

(a, b) -> 1
(a, a) -> 0.5
(b, b) -> 0.5
a 2
b 2

# a b
# a b c

(a, a) -> 0.5 +0.5    = 1
(a, b) -> 1   +1      = 2
(a, c) ->      0.5    = 0.5

(b, b) -> 0.5 +0.5    = 1
(b, c) -> 1           = 1

(c, c) -> 0.5         = 0.5

a 4.5
b 5
c 2.5

`````


Matrix: as a list of indexes `[i, j] -> val`
Each point in matrix:

```
i = i_shard*num_shards + i_offset

[i, j] -> [ (i_shard, j_shard), (i_offset, j_offset) ]

sort by (i_shard, j_shard)
```

--------------------------------


scala> val data = sc.parallelize(List((1, 2), (1, 1), (2, 3), (2, 1), (1, 4), (3, 5)), 2)
data: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> data.mapPartitions { _.map { println(_) } } collect
warning: there was one feature warning; re-run with -feature for details
[Stage 0:>                                                          (0 + 0) / 2]
(2,1)
(1,4)
(3,5)
(1,2)
(1,1)
(2,3)
res0: Array[Unit] = Array((), (), (), (), (), ())



val data = sc.parallelize(List((1, 2), (1, 1), (2, 3), (2, 1), (1, 4), (3, 5), (4,1)), 4)
data.getNumPartitions
val numShards = 2

def pprint(rdd: org.apache.spark.rdd.RDD[_]) = rdd.mapPartitionsWithIndex { (i, x) => {
   println(s"Partition #$i")
   x.map { element =>
    println(element)
   }}
}


val rp_data = data.repartitionAndSortWithinPartitions(new org.apache.spark.RangePartitioner(numShards, data))
val rps_data = data.repartitionAndSortWithinPartitions(new ShardPartitioner1(numShards))
val pby_data = data.partitionBy(new ShardPartitioner1(numShards))
rps_data.saveAsTextFile("/tmp/swivel_shards/repartitionAnd")
pby_data.saveAsTextFile("/tmp/swivel_shards/partitionBy")


class ShardPartitioner1(partitions: Int) extends org.apache.spark.Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = {
    val part = key.asInstanceOf[Int] % numPartitions
    println(s"Key: $key, partition: $part")
    part
  }

  override def equals(other: Any): Boolean = other match {
    case o: ShardPartitioner1 => o.numPartitions == numPartitions
    case _ =>  false
  }
  override def hashCode: Int = numPartitions.hashCode
}

val numShards = 2
val shards = for {
  i <- 0 to 2
  j <- 0 to 2
} yield ( (i%numShards, j%numShards), (i/numShards, j/numShards, 0.1) )

val data2 = sc.parallelize(shards, 2)

//(!!!)
//numPartition = numShards*numShards
val p_data2 = data2.repartitionAndSortWithinPartitions(new ShardPartitioner(numShards))
p_data2.saveAsTextFile("/tmp/swivel_shards/repartitionAnd2")


val pby_data2 = data2.partitionBy(new ShardPartitioner(numShards))
pby_data2.saveAsTextFile("/tmp/swivel_shards/partitionBy2")


  class ShardPartitioner(numShards: Int) extends  org.apache.spark.Partitioner {
    require(numShards >= 0, s"Number of partitions ($numShards) cannot be negative.")

    def numPartitions: Int = numShards*numShards
    def getPartition(key: Any): Int = key match {
      case null => 0
      case (i, j) => i.asInstanceOf[Int]*numShards + j.asInstanceOf[Int]
    }

    override def equals(other: Any): Boolean = other match {
      case o: ShardPartitioner => o.numPartitions == numPartitions
      case _ =>  false
    }
    override def hashCode: Int = numPartitions
  }

    def getPartition(key: Any): Int = key match {
      case null => 0
      case (i, j) => i.asInstanceOf[Int]*numShards + j.asInstanceOf[Int]
    }

shards.map { case (k, v) => println(s"$k -> " + getPartition(k)) }
data.mapPartitions { _.map { println(_) } } collect

-------------------------------------------------

    val unMergedCoocs = List(
	    ((1,1),0.5), ((1,1),0.5), ((1,1),0.5),
	    ((1,2),1.0), ((1,2),1.0), ((1,2),1.0),
	    ((1,3),0.5), ((1,3),0.5),
	    ((1,4),0.3333333333333333),
	    ((2,2),0.5), ((2,2),0.5), ((2,2),0.5),
	    ((2,3),1.0), ((2,3),1.0),
	    ((2,4),0.5),
	    ((3,3),0.5), ((3,3),0.5),
	    ((3,4),1.0),
	    ((4,4),0.5)
    )

val numShards = 2
val data  = sc.parallelize(unMergedCoocs, 3)
val p_data = data.repartitionAndSortWithinPartitions(new ShardPartitioner(numShards))

val merged = p_data.mapPartitions {
   _.toArray.groupBy(_._1).mapValues(_.map(_._2).sum).toIterator
}

i = k%N
j = k/N

