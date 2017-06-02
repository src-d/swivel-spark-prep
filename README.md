# Swivel Spark prep
> Distributed data preparation for Swivel model

Distributed equivalent of `prep.py` and `fastprep` from [Swivel](https://github.com/tensorflow/models/blob/master/swivel/) using Apache Spark.


# Development
```
./gradlew idea # if using InteliJ
./gradlew build
./gradlew test
```

# Run

On signle maching, Apache Spark in Local mode
```
./sparkprep --help
```

On Apache Spark standalone cluster
```
MASTER="<master-url>" ./sparkprep-cluster --help
```





