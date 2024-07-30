# kafka-sink-connector

## Env
- java 17
- gradle 8.9

## Configuration Properties
| Required | Name                      | Type   | Description                                                    | Sample        |
|----------|---------------------------|--------|----------------------------------------------------------------|---------------|
| O        | topics                    | String | A list of Kafka topics that the sink connector watches.        | my-topic      |
| O        | kafka.sink.topic          | String | The Kafka topic name to which the sink connector writes.       | relay-topic   |
| O        | kafka.sink.bootstrap      | String | The Kafka bootstrap server to which the sink connector writes. | my-kafka:9092 |
| X        | producer.max.request.size | Int    | The maximum size of a request in bytes.                        | 20971520      |

## Build
```
$ ./gradlew clean build
$ tar -czvf kafka-sink-connector.tar.gz app/build/libs/app-all.jar


$ rm ./kafka-sink-connector.tar.gz; rm app/build/libs/app-all.jar;./gradlew clean build;  tar -czvf kafka-sink-connector-dev.tar.gz app/build/libs/app-all.jar; rm /Users/kang/Desktop/workspace/labs/static/jars/kafka-sink-connector-dev.tar.gz; mv ./kafka-sink-connector-dev.tar.gz /Users/kang/Desktop/workspace/labs/static/jars/kafka-sink-connector-dev.tar.gz
```

# JAR (Java ARchive)
- JAR 파일은 자바 애플리케이션이나 라이브러리를 패키징하는 데 사용되는 파일 형식입니다. 여러 자바 클래스 파일, 메타데이터 및 리소스 파일을 포함할 수 있습니다.
- 장점:
- 배포 용이성: 여러 파일을 하나의 JAR 파일로 묶어 배포할 수 있어 편리합니다. 
- 실행 가능: 메타정보(Manifest)를 포함하여 실행 가능한 JAR 파일로 만들 수 있습니다.
- 클래스 로딩: JVM이 JAR 파일 내의 클래스들을 쉽게 로드할 수 있습니다.
- 표준화: 자바 애플리케이션 배포의 표준 형식입니다.
단점:
- 압축률 제한: 기본적으로 ZIP 형식으로 압축되며, gzip이나 다른 압축 방식에 비해 압축률이 낮을 수 있습니다.
- 유연성 부족: 파일 구조가 고정되어 있어, 다른 파일 형식에 비해 유연성이 떨어질 수 있습니다.
```
./gradlew clean build
```

# tar (Tape ARchive)
- TAR 파일은 여러 파일과 디렉터리를 하나의 아카이브 파일로 묶는 유닉스 기반의 파일 형식입니다. 자체적으로 압축 기능은 없습니다.
- 장점
- 유연성: 여러 파일과 디렉터리를 하나의 아카이브 파일로 쉽게 묶을 수 있습니다.
- 호환성: 유닉스 및 리눅스 시스템에서 널리 사용되며, 다양한 도구에서 지원됩니다.
- 보존: 파일 권한, 날짜, 디렉토리 구조 등을 보존합니다.
- 단점:
- 압축 부족: 자체적으로는 압축 기능이 없어 파일 크기가 줄어들지 않습니다.
```
tar -cvf connect.tar app/build/libs/app-all.jar
```
- c: create
- v: verbose
- f: file name
- z: gzip



# tar.gz
- TAR 파일을 gzip으로 압축한 파일 형식입니다. .tar.gz 또는 .tgz 확장자를 가집니다.
- 장점:
- 압축 효율성: gzip으로 압축하여 파일 크기를 크게 줄일 수 있습니다.
- 유연성 및 보존: TAR의 유연성과 보존 기능을 유지하면서, gzip의 높은 압축률을 추가로 얻을 수 있습니다.
- 표준화: 리눅스/유닉스 환경에서 널리 사용되는 표준 압축 형식입니다.
- 단점:
- 복잡성: 압축 및 해제 과정이 추가되어 다소 복잡할 수 있습니다.
- 압축 및 해제 시간: 압축 및 해제에 시간이 소요될 수 있습니다.
```
tar -czvf connect.tar.gz app/build/libs/app-all.jar
```
- c: create
- v: verbose
- f: file name
- z: gzip

