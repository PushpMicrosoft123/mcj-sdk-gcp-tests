package sf.mcj.main;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.pubsub.client.SubscriptionClient;
import com.salesforce.multicloudj.pubsub.client.TopicClient;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubMCJTest {

  private static final String PROJECT_ID = "sdv-demo-env";

  private static final String TOPIC = "mcj-topic";

  private static final String SUBSCRIPTION = "mcj-topic-sub";

  private static final String DLQ_SUBSCRIPTION = "mcj-dlq-sub";
  private static final Logger logger = LoggerFactory.getLogger(PubSubMCJTest.class);
  private static final int MAX_THREADS = 10;
  private static final ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);

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
    PS_SUB_AUTH_01();
    PS_SUB_AUTH_02();
    PS_SUB_REG_01();
    PS_SUB_MSG_01();
    PS_SUB_MSG_02();
    PS_SUB_MSG_03();
    PS_SUB_MSG_05();
    PS_SUB_MSG_07();
    PS_SUB_MSG_08();
    PS_SUB_MSG_09();
    PS_SUB_MSG_10();
    PS_SUB_MSG_11();
    PS_SUB_MSG_13();
    PS_SUB_MSG_14();
    PS_SUB_MSG_15();
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

      logger.info(String.format("[%s] Message published at regional endpoint. Test FAILED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test PASSED.", scenario));
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

      // Disconnect the WIFI or internet before running the below step
      tempTopicClient.send(message);
      //Now restore the network after 5 seconds before running next step.
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
    // Run below command to enable ADC before running this test.
    // gcloud auth application-default login
    String scenario = "PS_SUB_AUTH_01";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              .build();

      String textMessage = String.format("Test Message for %s", scenario);

      Message messageToBeSent = Message.builder()
              .withBody(textMessage.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              .build();
      Message message = subscriptionClient.receive();
      String data = new String(message.getBody());
      System.out.println("Received: " + data);
      subscriptionClient.sendAck(message.getAckID());
      subscriptionClient.close();

      logger.info(String.format("[%s] Test PASSED!!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_AUTH_02() {
    // Run below command to disable ADC before running this test.
    // gcloud auth application-default revoke
    String scenario = "PS_SUB_AUTH_02";
    try {
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              .build();
      //subscriptionClient.close();
      Message message = subscriptionClient.receive();
      String data = new String(message.getBody());
      System.out.println("Received: " + data);
      subscriptionClient.sendAck(message.getAckID());
      subscriptionClient.close();

      logger.info(String.format("[%s] Test FAILED!!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Exception Verified. Test PASSED.", scenario));
    }
  }

  private static void PS_SUB_REG_01() {
    //enable message Enforce in transit on topic before running this test.
    String scenario = "PS_SUB_REG_01";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              .withEndpoint(regionalEndpoint)
              .build();

      String textMessage = String.format("Test Message for %s", scenario);

      Message messageToBeSent = Message.builder()
              .withBody(textMessage.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              .withEndpoint(regionalEndpoint)
              .build();
      Message receivedMessage = subscriptionClient.receive();
      String data = new String(receivedMessage.getBody());
      System.out.println("Received: " + data);
      subscriptionClient.sendAck(receivedMessage.getAckID());
      subscriptionClient.close();

      logger.info(String.format("[%s] Test PASSED!!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_01() {
    String scenario = "PS_SUB_MSG_01";
    URI regionalEndpoint = URI.create("pubsub.us-central1.rep.googleapis.com:443");
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              .withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent = String.format("Test Message for %s", scenario);

      Message messageToBeSent = Message.builder()
              .withBody(textMessageSent.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              .withEndpoint(regionalEndpoint)
              .build();
      Message receivedMessage = subscriptionClient.receive();
      String receivedTextMessage = new String(receivedMessage.getBody());
      subscriptionClient.sendAck(receivedMessage.getAckID());
      subscriptionClient.close();

      if(textMessageSent.equals(receivedTextMessage)) {
        logger.info(String.format("[%s] Test PASSED!!", scenario));
      }
      else {
        logger.info(String.format("[%s] Message did not match. Test FAILED!!", scenario));
      }
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_02() {
    //Drain the subscription before running this test.
    String scenario = "PS_SUB_MSG_02";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              .build();

      String textMessageSent1 = String.format("Test Message 1 for %s", scenario);
      String textMessageSent2 = String.format("Test Message 2 for %s", scenario);
      String textMessageSent3 = String.format("Test Message 3 for %s", scenario);
      String textMessageSent4 = String.format("Test Message 4 for %s", scenario);
      String textMessageSent5 = String.format("Test Message 5 for %s", scenario);
      String textMessageSent6 = String.format("Test Message 6 for %s", scenario);

      Message messageToBeSent1 = Message.builder()
              .withBody(textMessageSent1.getBytes())
              .withMetadata("key1", "value1")
              .build();

      Message messageToBeSent2 = Message.builder()
              .withBody(textMessageSent2.getBytes())
              .withMetadata("key1", "value1")
              .build();

      Message messageToBeSent3 = Message.builder()
              .withBody(textMessageSent3.getBytes())
              .withMetadata("key1", "value1")
              .build();

      Message messageToBeSent4 = Message.builder()
              .withBody(textMessageSent4.getBytes())
              .withMetadata("key1", "value1")
              .build();

      Message messageToBeSent5 = Message.builder()
              .withBody(textMessageSent5.getBytes())
              .withMetadata("key1", "value1")
              .build();

      Message messageToBeSent6 = Message.builder()
              .withBody(textMessageSent6.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent1);
      tempTopicClient.send(messageToBeSent2);
      tempTopicClient.send(messageToBeSent3);
      tempTopicClient.send(messageToBeSent4);
      tempTopicClient.send(messageToBeSent5);
      tempTopicClient.send(messageToBeSent6);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();
      int count = 0;
      while (true) {
        Message message = subscriptionClient.receive();
        String data = new String(message.getBody());
        System.out.println("Received Message: " + data);
        subscriptionClient.sendAck(message.getAckID());
        count++;
        if(count == 6) {
          logger.error("[%s] Messages received. Test PASSED!", scenario);
          break;
        }
      }
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_03() {
    //Drain the subscription before running this test.
    String scenario = "PS_SUB_MSG_03";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent = String.format("Test Message for %s", scenario);

      Message messageToBeSent = Message.builder()
              .withBody(textMessageSent.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();

      // Disconnect the WIFI or internet before running the below step
      Message message = subscriptionClient.receive();
      //Now restore the network after 5 seconds before running next step.
      String data = new String(message.getBody());
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_05() {
    //Drain the subscription before running this test.
    String scenario = "PS_SUB_MSG_05";
    try {
      //send messages
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent1 = String.format("Test Message 1 for %s", scenario);
      String textMessageSent2 = String.format("Test Message 2 for %s", scenario);
      String textMessageSent3 = String.format("Test Message 3 for %s", scenario);
      String textMessageSent4 = String.format("Test Message 4 for %s", scenario);
      String textMessageSent5 = String.format("Test Message 5 for %s", scenario);
      String textMessageSent6 = String.format("Test Message 6 for %s", scenario);


      Message messageToBeSent1 = Message.builder()
              .withBody(textMessageSent1.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent2 = Message.builder()
              .withBody(textMessageSent2.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent3 = Message.builder()
              .withBody(textMessageSent3.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent4 = Message.builder()
              .withBody(textMessageSent4.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent5 = Message.builder()
              .withBody(textMessageSent5.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent6 = Message.builder()
              .withBody(textMessageSent6.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent1);
      tempTopicClient.send(messageToBeSent2);
      tempTopicClient.send(messageToBeSent3);
      tempTopicClient.send(messageToBeSent4);
      tempTopicClient.send(messageToBeSent5);
      tempTopicClient.send(messageToBeSent6);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();

      int count = 0;
      while (count < 6) {
        Message message = subscriptionClient.receive();
        executor.submit(() -> processMessageAndHandleNetwork(message, subscriptionClient));
        count++;
      }
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_07() {
    //Drain the subscription before running this test.
    String scenario = "PS_SUB_MSG_07";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent = String.format("Test Message for %s", scenario);

      Message messageToBeSent = Message.builder()
              .withBody(textMessageSent.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();
      int count = 0;
      while (true) {
        Message message = subscriptionClient.receive();
        String data = new String(message.getBody());
        subscriptionClient.sendAck(message.getAckID());
        count++;
        if(count == 2 && data.equals(textMessageSent)) {
          logger.error(String.format("[%s] Test FAILED as message was redelivered", scenario));
          break;
        }
      }
      logger.info(String.format("[%s] Test PASSED", scenario));
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_08() {
    //Drain the subscription before running this test.
    String scenario = "PS_SUB_MSG_08";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent = String.format("Test Message for %s", scenario);

      Message messageToBeSent = Message.builder()
              .withBody(textMessageSent.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();
      int count = 0;
      while (true) {
        Message message = subscriptionClient.receive();
        String data = new String(message.getBody());
        if(count == 2 && data.equals(textMessageSent)) {
          subscriptionClient.sendAck(message.getAckID());
          logger.info(String.format("[%s] Test Passed as message was redelivered", scenario));
          break;
        }
        subscriptionClient.sendNack(message.getAckID());
        count++;
      }

      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_10() {
    //Drain the subscription before running this test.
    //enable exactly once delivery on subscription
    //increase ack deadline to 60 sec on subscription
    //enable dead letter topic with subscription
    String scenario = "PS_SUB_MSG_10";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent1 = String.format("Test Message 1 for %s", scenario);
      String textMessageSent2 = String.format("Test Message 2 for %s", scenario);
      String textMessageSent3 = String.format("Test Message 3 for %s", scenario);
      String textMessageSent4 = String.format("Test Message 4 for %s", scenario);
      String textMessageSent5 = String.format("Test Message 5 for %s", scenario);
      String textMessageSent6 = String.format("Test Message 6 for %s", scenario);


      Message messageToBeSent1 = Message.builder()
              .withBody(textMessageSent1.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent2 = Message.builder()
              .withBody(textMessageSent2.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent3 = Message.builder()
              .withBody(textMessageSent3.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent4 = Message.builder()
              .withBody(textMessageSent4.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent5 = Message.builder()
              .withBody(textMessageSent5.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent6 = Message.builder()
              .withBody(textMessageSent6.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent1);
      tempTopicClient.send(messageToBeSent2);
      tempTopicClient.send(messageToBeSent3);
      tempTopicClient.send(messageToBeSent4);
      tempTopicClient.send(messageToBeSent5);
      tempTopicClient.send(messageToBeSent6);
      tempTopicClient.close();

      //prepare map
      HashMap<String, Integer> map = new HashMap<>();
      map.put(textMessageSent1, 0);
      map.put(textMessageSent2, 0);
      map.put(textMessageSent3, 0);
      map.put(textMessageSent4, 0);
      map.put(textMessageSent5, 0);
      map.put(textMessageSent6, 0);

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();

      //stop the fetch if set becomes empty
      while (true) {
        Message message = subscriptionClient.receive();
        String data = new String(message.getBody());
        System.out.println(data);
        subscriptionClient.sendNack(message.getAckID());
        map.put(data, map.get(data) + 1);
        //remove message
        List<String> result = map.entrySet()
                .stream()
                .filter(entry -> entry.getValue() < 4)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (result.isEmpty())
          break;

      }
      subscriptionClient.close();

      //check dead letter topic for 6 messages
      subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, DLQ_SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();

      int count = 0;
      while (true) {
        if(count == 5) {
          logger.info("[%s] Test Passed. Nacked message received");
          break;
        }
        Message message = subscriptionClient.receive();
        String data = new String(message.getBody());
        count++;
      }
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_09() {
    //Drain the subscription before running this test.
    String scenario = "PS_SUB_MSG_09";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent1 = String.format("Test Message 1 for %s", scenario);
      String textMessageSent2 = String.format("Test Message 2 for %s", scenario);
      String textMessageSent3 = String.format("Test Message 3 for %s", scenario);
      String textMessageSent4 = String.format("Test Message 4 for %s", scenario);
      String textMessageSent5 = String.format("Test Message 5 for %s", scenario);
      String textMessageSent6 = String.format("Test Message 6 for %s", scenario);


      Message messageToBeSent1 = Message.builder()
              .withBody(textMessageSent1.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent2 = Message.builder()
              .withBody(textMessageSent2.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent3 = Message.builder()
              .withBody(textMessageSent3.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent4 = Message.builder()
              .withBody(textMessageSent4.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent5 = Message.builder()
              .withBody(textMessageSent5.getBytes())
              .withMetadata("key1", "value1")
              .build();
      Message messageToBeSent6 = Message.builder()
              .withBody(textMessageSent6.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent1);
      tempTopicClient.send(messageToBeSent2);
      tempTopicClient.send(messageToBeSent3);
      tempTopicClient.send(messageToBeSent4);
      tempTopicClient.send(messageToBeSent5);
      tempTopicClient.send(messageToBeSent6);
      tempTopicClient.close();

      //prepare map
      HashSet<String> set = new HashSet<>();
      set.add(textMessageSent1);
      set.add(textMessageSent2);
      set.add(textMessageSent3);
      set.add(textMessageSent4);
      set.add(textMessageSent5);
      set.add(textMessageSent6);

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();

      //stop the fetch if set becomes empty
      while (true) {
        Message message = subscriptionClient.receive();
        String data = new String(message.getBody());
        if(!set.contains(data)) {
          logger.error("[%s] Test Failed as message was redelivered. Message was redelivered");
          break;
        }
        subscriptionClient.sendAck(message.getAckID());
        //remove message
        set.remove(data);
      }
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_11() {
    String scenario = "PS_SUB_MSG_11";
    try {
      //send message
      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      String textMessageSent = String.format("Test Message for %s", scenario);

      Message messageToBeSent = Message.builder()
              .withBody(textMessageSent.getBytes())
              .withMetadata("key1", "value1")
              .build();

      tempTopicClient.send(messageToBeSent);
      tempTopicClient.close();

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();
      Message message = subscriptionClient.receive();
      GetAttributeResult attributeResult = subscriptionClient.getAttributes();
      //attributeResult.getSchemaType() --- Not supported
      subscriptionClient.sendAck(message.getAckID());
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_13() {
    String scenario = "PS_SUB_MSG_13";
    try {
      int totalMessages = 50;

      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      //send messages
      for(int i =0; i < totalMessages; i++) {
        final int index = i;
        executor.submit(() -> {
          try {
            String textMessageSent = String.format("Test Message %d for %s", index, scenario);

            Message messageToBeSent = Message.builder()
                    .withBody(textMessageSent.getBytes())
                    .withMetadata("key1", "value1")
                    .withMetadata("messageIndex", String.valueOf(index))
                    .build();

            // Sending the message
            tempTopicClient.send(messageToBeSent);
            System.out.println("Sent message: " + index);

          } catch (Exception e) {
            System.err.println("Error sending message " + index + ": " + e.getMessage());
          }
        });
      }

      //receive message
      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();
      int count = totalMessages;
      while (count > 0) {
        Message message = subscriptionClient.receive();
        String data = new String(message.getBody());
        subscriptionClient.sendAck(message.getAckID());
        count--;
        System.out.println(count);
      }
      subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_14() {
    String scenario = "PS_SUB_MSG_14";
    try {
      int totalMessages = 500;

      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      //send messages
      for(int i =0; i < totalMessages; i++) {
        final int index = i;
        try {
          String textMessageSent = String.format("Test Message %d for %s", index, scenario);

          Message messageToBeSent = Message.builder()
                  .withBody(textMessageSent.getBytes())
                  .withMetadata("key1", "value1")
                  .withMetadata("messageIndex", String.valueOf(index))
                  .build();

          // Sending the message
          tempTopicClient.send(messageToBeSent);
          System.out.println("Sent message: " + index);

        } catch (Exception e) {
          System.err.println("Error sending message " + index + ": " + e.getMessage());
        }
      }

      SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
              .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
              //.withEndpoint(regionalEndpoint)
              .build();

      //receive messages

      for(int i =0; i < totalMessages; i++) {
        final int index = i;
        executor.submit(() -> {
          //while (true) {
          Message message = subscriptionClient.receive();
          String data = new String(message.getBody());
          subscriptionClient.sendAck(message.getAckID());
          System.out.println("Message received: " + data);
          //}
        });
      }
      //subscriptionClient.close();
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void PS_SUB_MSG_15() {
    String scenario = "PS_SUB_MSG_15";
    try {
      int totalMessages = 500;

      TopicClient tempTopicClient = TopicClient.builder("gcp")
              .withTopicName(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC))
              //.withEndpoint(regionalEndpoint)
              .build();

      //send messages
      for(int i =0; i < totalMessages; i++) {
        final int index = i;
        try {
          String textMessageSent = String.format("Test Message %d for %s", index, scenario);

          Message messageToBeSent = Message.builder()
                  .withBody(textMessageSent.getBytes())
                  .withMetadata("key1", "value1")
                  .withMetadata("messageIndex", String.valueOf(index))
                  .build();

          // Sending the message
          tempTopicClient.send(messageToBeSent);
          System.out.println("Sent message: " + index);

        } catch (Exception e) {
          System.err.println("Error sending message " + index + ": " + e.getMessage());
        }
      }

      //receive messages

      for(int i =0; i < 5; i++) {
        final int index = i;
        executor.submit(() -> {
          SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
                  .withSubscriptionName(String.format("projects/%s/subscriptions/%s", PROJECT_ID, SUBSCRIPTION))
                  //.withEndpoint(regionalEndpoint)
                  .build();

          while (true) {
            Message message = subscriptionClient.receive();
            String data = new String(message.getBody());
            subscriptionClient.sendAck(message.getAckID());
            System.out.println("Message received by subscription" + index + " client: " + data);
          }

          //subscriptionClient.close();
        });
      }
      //subscriptionClient.close();
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

  // --- Message Processing Logic ---
  private static void processMessageAndHandleNetwork(Message message, SubscriptionClient subscriptionClient) {

    try {
      // --- 2. Process Message Data ---
      String data = new String(message.getBody());
      System.out.println("Message received: " + data);
      // --- 4. Acknowledge Message ---
      subscriptionClient.sendAck(message.getAckID());

    } catch (Exception e) {
      throw e;
    }
  }

}