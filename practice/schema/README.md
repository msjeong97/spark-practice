# Spark DataFrame의 Schema

## Schema-On-Read
- 대부분 schema-on-read는 잘 작동한다.
- 하지만 `Long` data type을 `Integer` data type으로 잘못 inference 할 수 있고, schema를 직접 지정해야할 필요가 있다.


## Define Schema
- 아래와 같이 schema를 지정할 수 있다.
```scala
StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", DoubleType, true)
))
```
- `StructField` 는 column name, column type, nullable 값이 포함된다.
- 필요한 경우 아래와 같이 컬럼 관련 메타데이터를 추가할 수 있다. 
```scala
StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", DoubleType, true, Metadata.fromJson("{\"hello\":\"world\"}"))
))
```
