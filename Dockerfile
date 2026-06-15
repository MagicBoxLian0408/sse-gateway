FROM eclipse-temurin:21-jre-jammy
ARG JAR_FILE=build/libs/*.jar
RUN groupadd -r appuser && useradd -r -g appuser appuser
WORKDIR /app
COPY ${JAR_FILE} app.jar
RUN chown -R appuser:appuser /app
USER appuser
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
