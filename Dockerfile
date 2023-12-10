FROM bitnami/spark:latest

USER root

# Install wget
RUN apt-get update && apt-get install -y wget

# Install Hadoop AWS, AWS Java SDK JARs
RUN wget -P /usr/local/spark/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.3/hadoop-aws-3.2.3.jar \
 && wget -P /usr/local/spark/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1032/aws-java-sdk-bundle-1.11.1032.jar

# Install Delta Lake package
RUN pip install --no-cache-dir delta-spark==2.2.0

USER $NB_UID

