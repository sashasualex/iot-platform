FROM eclipse-temurin:24-jre
WORKDIR /app
COPY build/libs/events-collector-service-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar", "--spring.profiles.active=docker"]
