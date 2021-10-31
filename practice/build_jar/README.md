# SBT를 사용한 scala application 빌드 과정

## spark application 작성
- scala로 작성

## build.sbt 작성
- IntelliJ > File > new > Project ... > Scala > sbt 로 project 를 새로 만드는게 편함.
- application name, application version, scala version 작성
- libraryDependency 추가 

## compile
```bash
$ sbt package
```
- `./target/scala-2.12/` 에 .jar 파일이 생성됨

## 실행
```bash
$ ./bin/spark-submit --class SparkScalaExample /path/to/examples.jar
```

