package sf.mcj.main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sf.mcj.main.TestData.CustomMetadata;
public class BlobStoreMCJTest {

  private static BucketClient bucketClient;

  private static final Logger logger = LoggerFactory.getLogger(BlobStoreMCJTest.class);

  public static void main(String[] args) {
    //****** Please recreate the bucket mcj-blob-test before running the test cases*****

    if(!initializeBucketClient()) {
      logger.error("GCS Client initialization failed. Exiting...");
    }

    logger.info("---- Running GCS Tests-----");

    GCS_UPLOAD_01();

    GCS_UPLOAD_02();

    GCS_UPLOAD_03();

    GCS_UPLOAD_04();

    GCS_UPLOAD_05();

    GCS_UPLOAD_06();

    GCS_UPLOAD_07();

    GCS_UPLOAD_08();

    GCS_UPLOAD_09();

    GCS_UPLOAD_10();

    GCS_UPLOAD_11();

    GCS_UPLOAD_12();

    GCS_UPLOAD_13();

    GCS_UPLOAD_15();
  }

  public static void GCS_UPLOAD_01() {
    String scenario = "GCS_UPLOAD_01";

    try {
      InputStream content = new ByteArrayInputStream("Hello World!".getBytes());
      String objectName = UUID.randomUUID().toString();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of("GCS_UPLOAD_CONTENT_01", "GCS_UPLOAD_CONTENT_01_VAL"))
          .build();
      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, content);
      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] Content uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_02() {
    String scenario = "GCS_UPLOAD_02";

    try {
      String objectName = UUID.randomUUID().toString();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of("GCS_UPLOAD_02", "GCS_UPLOAD_02"))
          .build();
      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, "Hello World!".getBytes());
      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] Content uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_03() {
    String scenario = "GCS_UPLOAD_03";
    String objectName = UUID.randomUUID().toString();
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      createFile("Hello World!", objectName);
      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, new File(objectName));

      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] File uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_UPLOAD_04() {
    String scenario = "GCS_UPLOAD_04";

    try {
      InputStream content = new ByteArrayInputStream(generateLargeString(10).getBytes());
      String objectName = UUID.randomUUID().toString();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, content);
      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] Content uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_05() {
    String scenario = "GCS_UPLOAD_05";

    try {
      String objectName = UUID.randomUUID().toString();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, generateLargeString(10).getBytes(
          StandardCharsets.UTF_8));
      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] Content uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_06() {
    String scenario = "GCS_UPLOAD_06";
    String objectName = UUID.randomUUID().toString();
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      createFile(generateLargeString(100), objectName);
      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, new File(objectName));
      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] File uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_UPLOAD_07() {
    String scenario = "GCS_UPLOAD_07";

    try {
      String objectName = UUID.randomUUID().toString();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s", String.format("%s/%s",scenario,objectName)))
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object.", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, "Hello World!".getBytes());

      uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s", String.format("%s/%s",scenario,objectName)))
          .withMetadata(Map.of(scenario, String.format("%s_VAL_APPEND", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object", scenario));
      response = bucketClient.upload(uploadRequest, "Hello World New!".getBytes());

      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] Content uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_08() {
    String scenario = "GCS_UPLOAD_08";

    try {
      String objectName = UUID.randomUUID().toString();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("ab\ncd.txt", objectName))
          .withMetadata(Map.of(scenario, String.format("%s_VAL_APPEND", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object...(expected to fail)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, "Hello World!".getBytes(
          StandardCharsets.UTF_8));
      logger.info("received upload response {}", response);
    } catch (InvalidArgumentException e) {
      logger.info("[%s] Exception Verified. Test passed.", scenario);
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_09() {
    String scenario = "GCS_UPLOAD_09";

    try {
      String objectName = UUID.randomUUID().toString();
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, generateLargeString(10)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object...(expected to fail)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, "Hello World!".getBytes(
          StandardCharsets.UTF_8));
      logger.info("received upload response {}", response);
    } catch (InvalidArgumentException e) {
      logger.info("[%s] Exception Verified. Test passed.", scenario);
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_10() {
    String scenario = "GCS_UPLOAD_10";
    String objectName = UUID.randomUUID().toString();
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      createFile("", objectName);
      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, new File(objectName));

      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] File uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_UPLOAD_11() {
    String scenario = "GCS_UPLOAD_11";

    try {
      String objectName = UUID.randomUUID().toString();
      ObjectMapper objectMapper = new ObjectMapper();
      CustomMetadata customMetadata = CustomMetadata.builder().id(1).name(scenario).build();

      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, objectMapper.writeValueAsString(customMetadata)))
              .build();

      logger.info(String.format("[%s] Uploading content to non-existing object...(expected to fail)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, "Hello World!".getBytes(
          StandardCharsets.UTF_8));
      logger.info("received upload response {}", response);
    } catch (InvalidArgumentException e) {
      logger.info("[%s] Exception Verified. Test passed.", scenario);
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_12() {
    String scenario = "GCS_UPLOAD_12";

    try {
      String objectName = UUID.randomUUID().toString();
      ObjectMapper objectMapper = new ObjectMapper();
      CustomMetadata customMetadata = CustomMetadata.builder().id(1).name(scenario).build();

      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("%s.txt", objectName))
          .withMetadata(Map.of(scenario, objectMapper.writeValueAsString(customMetadata)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object...(expected to fail)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, new File(String.format("%s.txt", scenario)));
      logger.info("received upload response {}", response);
    } catch (IOException e) {
      logger.info("[%s] Exception Verified. Test passed.", scenario);
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_13() {
    String scenario = "GCS_UPLOAD_13";

    try {
      generateTestFile("test1.txt", 1);
      generateTestFile("test2.txt", 5);
      generateTestFile("test3.txt", 10);
      generateTestFile("test4.txt", 1);
      generateTestFile("test5.txt", 5);
      generateTestFile("test6.txt", 10);
      generateTestFile("test7.txt", 1);
      generateTestFile("test8.txt", 5);
      generateTestFile("test9.txt", 10);
      generateTestFile("test10.txt", 10);

      List<String> filesToUpload = new ArrayList<>();
      filesToUpload.add("test1.txt");
      filesToUpload.add("test2.txt");
      filesToUpload.add("test3.txt");
      filesToUpload.add("test4.txt");
      filesToUpload.add("test5.txt");
      filesToUpload.add("test6.txt");
      filesToUpload.add("test7.txt");
      filesToUpload.add("test8.txt");
      filesToUpload.add("test9.txt");
      filesToUpload.add("test10.txt");

      int numberOfConcurrentUploads = 20; // Adjust based on your testing needs

      ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentUploads);
      List<Future<Boolean>> futures = new ArrayList<>();

      for (String filePath : filesToUpload) {
        Callable<Boolean> uploadTask = () -> {
          String objectName = Paths.get(filePath).getFileName().toString();
          UploadRequest uploadRequest = new UploadRequest.Builder()
              .withKey(objectName)
              .withMetadata(Map.of("key", "value"))
              .build();

          try {
            byte[] fileContent = Files.readAllBytes(Paths.get(filePath));
            logger.info(String.format("[%s] Uploading content to the file...", scenario));
            UploadResponse response = bucketClient.upload(uploadRequest, fileContent);
            return true;
          } catch (Exception e) {
            logger.error("Failed to upload " + filePath + ": " + e.getMessage());
            return false;
          }
        };
        futures.add(executor.submit(uploadTask));
      }

      // Wait for all tasks to complete and check results
      int successfulUploads = 0;
      for (Future<Boolean> future : futures) {
        try {
          if (future.get()) { // .get() blocks until the task is complete
            successfulUploads++;
          }
        } catch (ExecutionException e) {
          logger.error("Task execution failed: " + e.getMessage());
        }
      }

      executor.shutdown(); // Shut down the executor
      //executor.awaitTermination(1, Duration.ofMinutes(100));

     if(successfulUploads != filesToUpload.size()) {
       logger.error(String.format("[%s] Test FAILED.", scenario));
     }
     else {
       logger.info(String.format("[%s] Data verified. Test PASSED.", scenario));
     }
      for (String filePath : filesToUpload) {
        Files.deleteIfExists(Paths.get(filePath));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_UPLOAD_15() {
    String scenario = "GCS_UPLOAD_15";
    String objectName = String.format("%s.txt", UUID.randomUUID().toString());
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      createFile(generateLargeString(100), objectName);
      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, new File(objectName));

      logger.info("received upload response {}", response);
      logger.info(String.format("[%s] File uploaded. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  private static void generateTestFile(String filename, long sizeMB) throws IOException {
    Path filePath = Paths.get(filename);
    byte[] buffer = new byte[1024 * 1024];
    Random random = new Random();

    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath.toFile())) {
      for (int i = 0; i < sizeMB; i++) {
        random.nextBytes(buffer);
        fos.write(buffer);
      }
    }
  }

  private static String generateLargeString(int sizeInMB) {
    // 1 MB = 1024 * 1024 bytes
    long targetBytes = (long) sizeInMB * 1024 * 1024;
    StringBuilder sb = new StringBuilder();
    char charToRepeat = 'a';

    for (long i = 0; i < targetBytes; i++) {
      sb.append(charToRepeat);
    }

    return sb.toString();
  }

  private static void createFile(String content, String fileName) {
    try (FileWriter fileWriter = new FileWriter(fileName)) {
      fileWriter.write(content);
    } catch (IOException e) {
      logger.error("An error occurred while writing with FileWriter: " + e.getMessage());
    }
  }

  private static void deleteFile(String path) {
    Path pathNIO = Paths.get(path);
    try {
      Files.delete(pathNIO);
      System.out.println(path + " deleted successfully using Files.delete().");
    } catch (IOException e) {
      logger.error("Error deleting " + path + ": " + e.getMessage());
    }
  }

  private static boolean initializeBucketClient() {
    try {
      bucketClient = BucketClient.builder("gcp")
          .withRegion("us-east1")
          .withBucket("mcj-blob-test")
          .build();
      return true;
    }
    catch (SubstrateSdkException sse) {
      logger.info("bucketClient connection failed. SDK caught exception raised with error" + sse.getMessage());
      return false;
    }
    catch (Exception ex) {
      logger.info("bucketClient connection failed. SDK uncaught exception raised with error" + ex.getMessage());
      return false;
    }
  }
  }