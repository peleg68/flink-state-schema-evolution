# Flink State Schema Evolution POC
This is an example of using Avro schema generated classes and Flink's Avro serialization to enable restoring a Flink job from a previous version with a different element schema.

## How to try it yourself
This repository has 2 branches:
- `main`
- `main-previous`

To begin follow these steps:
1. Clone branch `main`
2. Run `mvn package`. A JAR called `flink-state-schema-evolution-1.5.10.jar` will appear in `target` folder. Keep the JAR and do not run `mvn clean`.
3. Checkout branch `main-previous`.
4. Run `mvn package`. A JAR called `flink-state-schema-evolution-1.4.10.jar` will appear in `target` folder.
5. Run `docker compose up` in order to start a Flink cluster in session mode.
6. Enter http://localhost:8081/ to enter the Flink dashboard.
7. Upload the JAR called `flink-state-schema-evolution-1.4.10.jar` and submit it with your chosen parallelism.
8. When the job starts running, copy the job ID from the dashboard.
9. Run the command `docker ps` and copy the jobmanager container name.
10. Stop the job with a savepoint using the command:
    ```bash
    docker exec {YOUR_JOBMANAGER_CONTAINER_NAME} /opt/flink/bin/flink stop {YOUR_JOB_ID} -p file:/opt/flink/savepoints/
    ```
11. Copy the savepoint path from the command output.
12. Enter http://localhost:8081/ to enter the Flink dashboard.
13. Upload the JAR called `flink-state-schema-evolution-1.5.10.jar` and submit it with your chosen parallelism and the savepoint path you copied.
14. When the job starts running - it means you successfully restored the job in version 1.5.10 from a savepoint you created with version 1.4.10.
