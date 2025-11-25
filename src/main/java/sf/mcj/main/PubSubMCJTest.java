package sf.mcj.main;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.client.SubscriptionClient;
import com.salesforce.multicloudj.pubsub.client.TopicClient;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubMCJTest {

  private static final String PROJECT_ID = "sdv-demo-env";

  private static final String TOPIC = "mcj-topic";

  private static final String SUBSCRIPTION = "mcj-topic-sub";
  private static final Logger logger = LoggerFactory.getLogger(PubSubMCJTest.class);

  public static void main(String[] args) throws IOException {
     PS_PUB_AUTH_01();
     PS_PUB_AUTH_02();
     PS_PUB_REG_01();
     PS_PUB_REG_02();
     PS_PUB_MSG_01();
     PS_PUB_MSG_03();
     PS_PUB_MSG_04();
     PS_PUB_MSG_05();
     PS_PUB_MSG_06();
     PS_PUB_MSG_07();
     PS_PUB_MSG_08();
     PS_PUB_MSG_09();
     PS_PUB_MSG_10();
  }

  private static void PS_PUB_AUTH_01() {
    // Run below command to enable ADC before running this test.
    // gcloud auth application-default login
    String scenario = "PS_PUB_AUTH_01";
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .build();

      Message message = Message.builder()
          .withBody("Hello, testing connectivity!".getBytes())
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();
      logger.info(String.format("[%s] Authentication verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_AUTH_02() {
    // Run below command to disable ADC before running this test.
    // gcloud auth application-default revoke
    String scenario = "PS_PUB_AUTH_02";
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .build();

      Message message = Message.builder()
          .withBody("Hello, testing connectivity!".getBytes())
          .build();

      tempTopicClient.send(message);
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    catch (UnknownException e) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    }
  }

  private static void PS_PUB_REG_01() {
    //enable message Enforce in transit on topic before running this test.
    String scenario = "PS_PUB_REG_01";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody("Hello, testing connectivity!".getBytes())
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.info(String.format("[%s] Message published at regional endpoint. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_REG_02() {
    //disable message "Enforce in transit" for topic on console before running this test.
    String scenario = "PS_PUB_REG_02";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody("Hello, testing connectivity!".getBytes())
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.info(String.format("[%s] Message published at regional endpoint. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_01() {
    String scenario = "PS_PUB_MSG_01";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody("Hello, testing connectivity!".getBytes())
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.info(String.format("[%s] Message published at regional endpoint. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_03() {
    String scenario = "PS_PUB_MSG_03";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody(get9MBMessage())
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.info(String.format("[%s] Large Message published at regional endpoint. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_04() {
    String scenario = "PS_PUB_MSG_04";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody(get11MBMessage())
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    catch (InvalidArgumentException e) {
      logger.info(String.format("[%s] Exception Verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_05() {
    String scenario = "PS_PUB_MSG_05";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody("Hello, sending metadata".getBytes())
          .withMetadata(Map.of("key1", "value1", "key2", "value2"))
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.info(String.format("[%s] Large Message published at regional endpoint. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_06() {
    String scenario = "PS_PUB_MSG_06";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      List<Message> messages = List.of(Message.builder()
          .withBody("Hello, sending message 1".getBytes())
          .withMetadata(Map.of("key1", "value1", "key2", "value2"))
          .build(),
          Message.builder()
          .withBody("Hello, sending message 2".getBytes())
          .withMetadata(Map.of("key1", "value1", "key2", "value2"))
          .build(),
          Message.builder()
              .withBody("Hello, sending message 3".getBytes())
              .withMetadata(Map.of("key1", "value1", "key2", "value2"))
              .build(),
          Message.builder()
              .withBody("Hello, sending message 4".getBytes())
              .withMetadata(Map.of("key1", "value1", "key2", "value2"))
              .build(),
          Message.builder()
              .withBody("Hello, sending message 5".getBytes())
              .withMetadata(Map.of("key1", "value1", "key2", "value2"))
              .build(),
          Message.builder()
              .withBody("Hello, sending message 6".getBytes())
              .withMetadata(Map.of("key1", "value1", "key2", "value2"))
              .build());

      for (Message message: messages) {
        tempTopicClient.send(message);
      }
      tempTopicClient.close();
      logger.info(String.format("[%s] Messages published at regional endpoint. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_07() {
    String scenario = "PS_PUB_MSG_07";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, "dummy"))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody("Publishing on wrong topic".getBytes())
          .withMetadata(Map.of("key1", "value1", "key2", "value2"))
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.error(String.format("[%s] Test FAILED!", scenario));
    }
    catch (ResourceNotFoundException e) {
      logger.error(String.format("[%s] Exception Verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_08() {
    String scenario = "PS_PUB_MSG_08";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody("".getBytes())
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.error(String.format("[%s] Message Published. Test Failed!", scenario));
    }
    catch (InvalidArgumentException e) {
      logger.info(String.format("[%s] Exception Verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_09() {
    String scenario = "PS_PUB_MSG_09";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .withEndpoint(regionalEndpoint)
          .build();

      Message message = Message.builder()
          .withBody("".getBytes())
          .withMetadata("key1", "value1")
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.info(String.format("[%s] Test PASSED!!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_PUB_MSG_10() {
    String scenario = "PS_PUB_MSG_10";
    try {
      TopicClient tempTopicClient = TopicClient.builder("gcp")
          .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
          .build();

      Message message = Message.builder()
          .withBody("Test Message".getBytes())
          .withMetadata("key1", "value1")
          .build();

      tempTopicClient.send(message);
      tempTopicClient.close();

      logger.info(String.format("[%s] Test PASSED!!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_AUTH_01() {
    String scenario = "PS_SUB_AUTH_01";
    URI endpoint = URI.create("https://pubsub.googleapis.com/v1");
    try {
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
          .withSubscriptionName("projects/sdv-demo-env/subscriptions/mcj-topic-sub")
          .build();
      subscriptionClient.close();

    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static byte[] get9MBMessage() {
    final int TOO_LARGE_SIZE_BYTES = 9 * 1024 * 1024;

    byte[] largeDataBytes = new byte[TOO_LARGE_SIZE_BYTES];
    java.util.Arrays.fill(largeDataBytes, (byte) 'B');
    return largeDataBytes;
  }

  public static byte[] get11MBMessage() {
    final int TOO_LARGE_SIZE_BYTES = 11 * 1024 * 1024;

    byte[] largeDataBytes = new byte[TOO_LARGE_SIZE_BYTES];
    java.util.Arrays.fill(largeDataBytes, (byte) 'B');
    return largeDataBytes;
  }

}
