# Spark DataFrame과 SQL사용

## `createOrReplaceTempView`
- Spark SQL을 사용하면 모든 DataFrame을 테이블이나 뷰로 등록한 후 SQL 쿼리를 사용할 수 있다.
```scala
val df = spark.read("...")
df.createOrReplaceTempView("table_name")
```

## spark.sql
- 아래와 같이 쿼리를 사용할 수 있다. 
- DataFrame에 쿼리를 수행시, 새로운 DataFrame을 리턴 한다.
```scala
spark.sql("""SELECT SUM(cnt) FROM table_name""")
```
