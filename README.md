# Swivel Spark prep
> Distributed data preparation for Swivel model

Distributed equivalent of `prep.py` and `fastprep` from [Swivel](https://github.com/tensorflow/models/blob/master/swivel/) using Apache Spark.


# Development
```
./gradlew idea # if using InteliJ Idea
./gradlew build
./gradlew test
```

# Run

On signle maching, Apache Spark in Local mode
```
./gradlew shadowJar
java -jar build/libs/swivel-spark-0.0.1-all.jar <path-to-file>
```

On Apache Spark cluster
```
./gradlew build

spark-submit \
  --packages tapanalyticstoolkit:spark-tensorflow-connector:1.0.0-s_2.11 \
  --class com.srcd.swivel.SparkPrepDriver \
  swivel-spark-0.0.1.jar \
  hdfs://<path-to-file>
```

See [official instructions](https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector#using-spark-shell) on how to run with `--jar` instead of `--packages`