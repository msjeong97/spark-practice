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
- SQL-like하지 않게 사용도 가능
```scala
df.where(col("ORIGIN_COUNTRY_NAME") === "United States")
df.where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
```

## orderBy
- column 조건에 따라 정렬된 dataframe을 리턴
- `desc`, `asc`를 사용 가 
```scala
df.where("count > 100").orderBy(desc("count"))
```

## select
- sql과 마찬가지로 특정 column을 select한 dataframe을 리턴
```scala
df.select("DEST_COUNTRY_NAME")
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
```
- column을 사용할 때(select, where, ...) 아래와 같이 column을 사용할 수 있음적
```scala
df.select("DEST_COUNTRY_NAME") // spark의 implicit 변환(string -> column)이 적용됨
df.select($"DEST_COUNTRY_NAME")
df.select(col("DEST_COUNTRY_NAME"))
```

## distinct
- sql과 마찬가지로 구분되는 row들의 집합(dataframe)을 리턴
```scala
df.select("DEST_COUNTRY_NAME").distinct()
```

## union
- sql과 마찬가지로 두 dataframe을 union한 dataframe을 리턴
```scala
df1.union(df2)
```
