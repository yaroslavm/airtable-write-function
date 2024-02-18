package functions;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AirtableWriteFunction implements BackgroundFunction<Message> {

  private static final Gson gson = new Gson();

  private String getEnv(final String key) {
    final var value = System.getenv(key);
    if (value == null) {
      throw new IllegalArgumentException(key + " is null");
    }
    return value;
  }

  @Override
  public void accept(final Message message, final Context context) throws Exception {
    final var airtableToken = getEnv("AIRTABLE_TOKEN");
    final var airtableDatabase = getEnv("AIRTABLE_DATABASE");
    final var airtableTable = getEnv("AIRTABLE_TABLE");

    log.info(
        "Message arrived with id: {}, publishTime: {}, attributes: {}, context: {}",
        message.getMessageID(),
        message.getPublishTime(),
        message.getAttributes(),
        context);
    if (message.getData() == null) {
      log.warn("field `data` is null, exiting.");
      return;
    }
    final var rawDataDecoded =
        new String(Base64.getDecoder().decode(message.getData()), StandardCharsets.UTF_8);
    final var dataObj = gson.fromJson(rawDataDecoded, Object.class);
    log.info("dataObj received: {}", dataObj);

    //noinspection unchecked
    final var airRecord =
        writeRecord(
            airtableToken,
            AirtableTriggerEvent.of(
                airtableDatabase, airtableTable, (Map<String, String>) dataObj));
    log.info("record with id {} has been created.", airRecord.id);
  }

  private static AirRecord writeRecord(String airtableToken, AirtableTriggerEvent event)
      throws IOException, InterruptedException {
    final var httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.of(5, ChronoUnit.SECONDS)).build();
    final var url = String.format("https://api.airtable.com/v0/%s/%s", event.database, event.table);

    final var jsonBody = new JsonObject();
    jsonBody.add("fields", gson.toJsonTree(event.fields));
    final var request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + airtableToken)
            .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(jsonBody)))
            .build();
    final var httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    final var body = httpResponse.body();
    return gson.fromJson(body, AirRecord.class);
  }

  @Data
  @AllArgsConstructor(staticName = "of")
  public static class AirtableTriggerEvent {
    String database;
    String table;
    Map<String, String> fields;
  }

  @Data
  public static class AirRecord {
    String id;
    String createdTime;
    Map<String, String> fields;
  }
}
