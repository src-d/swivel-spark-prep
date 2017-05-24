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
./sparkprep --help
```

On Apache Spark cluster
```
./sparkprep-cluster --help
```

See [official instructions](https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector#using-spark-shell) on how to run with `--jar` instead of `--packages`