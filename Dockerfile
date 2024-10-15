FROM openjdk:21-jdk-slim
WORKDIR /recommendations
COPY /target/recommendations-1.0.0.jar /recommendations
CMD ["java", "-jar", "recommendations-1.0.0.jar"]
