FROM apache/spark:python3

# S3A 커넥터 JAR 다운로드 (S3 읽기/쓰기에 필요)
# hadoop-aws 3.4.1 + AWS SDK V2 bundle
USER root
RUN curl -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -o /opt/spark/jars/bundle-2.29.38.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.38/bundle-2.29.38.jar

# boto3 설치 (S3 폴더 목록 조회용)
RUN pip install boto3 --no-cache-dir

USER spark