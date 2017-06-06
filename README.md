# Swivel Spark prep [![Build Status](https://travis-ci.org/src-d/swivel-spark-prep.svg?branch=master)](https://travis-ci.org/src-d/swivel-spark-prep)
> Distributed data preparation for the Swivel model

Distributed equivalent of `prep.py` and `fastprep` from [Swivel](https://github.com/tensorflow/models/blob/master/swivel/) using Apache Spark.


# Development
```
./gradlew idea # if using InteliJ
./gradlew build
./gradlew test
```

# Run

On a single machine, Apache Spark in Local mode
```
./gradlew shadowJar
./sparkprep --help
```

On an Apache Spark standalone cluster
```
./gradlew build

# https://github.com/tensorflow/ecosystem/tree/master/hadoop#build-and-install
# cp <path-to-ecosystem-hadoop>/target/tensorflow-hadoop-1.0-SNAPSHOT-shaded-protobuf.jar .

MASTER="<master-url>" ./sparkprep-cluster --help
```
