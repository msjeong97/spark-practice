# Spark DataFrame API

## withColumn
- 새로운 column을 추가한 dataframe을 리턴
```scala
df.withColumn(COLUMN_NAME, COLUMN_VALUE)
df.withColumn("count as string", col("count").cast("string"))
```

## withColumnRenamed
- column 명을 수정한 dataframe을 리턴
```scala
df.withColumnRenamed(ORIGIN_COLUMN_NAME, DEST_COLUMN_NAME)
```

## filter
- 특정 조건 row들을 filtering 한 dataframe을 리턴
- equal 조건에서 `=`를 세개 사용
```scala
df.filter(col("ORIGIN_COUNTRY_NAME") === "United States")
df.filter(col("count") > 100)
```

## where
- SQL의 where과 유사하게 사용 가능
- 위 filter 예제와 동치
```scala
df.where("ORIGIN_COUNTRY_NAME == \"United States\"")
df.where("count > 100")
```

## orderBy
- column 조건에 따라 정렬된 dataframe을 리턴
- `desc`, `asc`를 사용 가 
```scala
df.where("count > 100").orderBy(desc("count"))
```
