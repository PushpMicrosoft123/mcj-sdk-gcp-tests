package sf.mcj.main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.PresignedOperation;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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

  private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private static final SecureRandom RANDOM = new SecureRandom();

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

    GCS_DOWNLOAD_01();

    GCS_DOWNLOAD_02();

    GCS_DOWNLOAD_03();

    GCS_DOWNLOAD_04();

    GCS_DOWNLOAD_07();

    GCS_DOWNLOAD_08();

    GCS_DOWNLOAD_09();

    GCS_DOWNLOAD_10();

    GCS_DELETE_01();

    GCS_DELETE_02();

    GCS_DELETE_03();

    GCS_DELETE_04();

    GCS_COPY_01();

    GCS_COPY_02();

    GCS_COPY_03();

    GCS_METADATA_01();

    GCS_LIST_OBJECTS_01();

    GCS_PRESIGNED_URL_01();

    GCS_PRESIGNED_URL_02();

    GCS_MPU_01();

    GCS_MPU_02();

    GCS_MPU_03();

    GCS_DOWNLOAD_11();

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

  public static void GCS_DOWNLOAD_01() {
    String scenario = "GCS_DOWNLOAD_01";
    String objectName = String.format("%s.txt", scenario);
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      String expectedContent = " This is a download test with trailing space and word café ";
      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, expectedContent.getBytes(
          StandardCharsets.UTF_8));
      logger.info("received upload response {}", response);

      logger.info(String.format("[%s] Validating the content.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectName)
          .withVersionId(response.getVersionId())
          .build();
      ByteArray downloadedBytes = new ByteArray();
      bucketClient.download(downloadRequest,downloadedBytes);
      byte[] actualBytes = downloadedBytes.getBytes();
      String actualContent = new String(actualBytes);
      if(expectedContent.equals(actualContent)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
      else {
        logger.error(String.format("[%s] Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_DOWNLOAD_02() {
    String scenario = "GCS_DOWNLOAD_02";
    String objectName = String.format("%s.txt", scenario);
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      String expectedContent = "Hello, this is a test line.\n" +
          "This is another line of text.";

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, expectedContent.getBytes(
          StandardCharsets.UTF_8));
      logger.info("received upload response {}", response);

      logger.info(String.format("[%s] Validating the content.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectName)
          .withVersionId(response.getVersionId())
          .build();
      OutputStream outputStream = new FileOutputStream(objectName);
      bucketClient.download(downloadRequest,outputStream);
      String actualContent = Files.readString(Paths.get(objectName), StandardCharsets.UTF_8);
      if(expectedContent.equals(actualContent)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
      else {
        logger.error(String.format("[%s] Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_DOWNLOAD_03() {
    String scenario = "GCS_DOWNLOAD_03";
    String objectName = String.format("%s.txt", scenario);
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      String expectedContent = "Hello, this is a test line.\n" +
          "This is another line of text.";
      Path path = Paths.get(objectName);
      Files.write(path, "Hello World!".getBytes(StandardCharsets.UTF_8));

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, expectedContent.getBytes(
          StandardCharsets.UTF_8));
      logger.info("received upload response {}", response);

      logger.info(String.format("[%s] Validating the content.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectName)
          .withVersionId(response.getVersionId())
          .build();
      //OutputStream outputStream = new FileOutputStream(objectName);
      bucketClient.download(downloadRequest, path);

      String actualContent = Files.readString(path, StandardCharsets.UTF_8);
      if(expectedContent.equals(actualContent)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
      else {
        logger.error(String.format("[%s] Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_DOWNLOAD_04() {
    String scenario = "GCS_DOWNLOAD_04";
    String objectName = String.format("%s.txt", scenario);
    String downloadedObjectName = String.format("%s-download.txt", scenario);
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();
      createFile(generateLargeString(100), objectName);

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, new File(objectName));

      logger.info(String.format("[%s] Downloading the file.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectName)
          .withVersionId(response.getVersionId())
          .build();
      bucketClient.download(downloadRequest, Paths.get(downloadedObjectName));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
      deleteFile(downloadedObjectName);
    }
  }

  public static void GCS_DOWNLOAD_07() {
    String scenario = "GCS_DOWNLOAD_07";
    String objectName = String.format("%s.txt", scenario);
    try {

      logger.info(String.format("[%s] Downloading the file.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectName)
          .build();
      bucketClient.download(downloadRequest, Paths.get(objectName));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_DOWNLOAD_08() {
    String scenario = "GCS_DOWNLOAD_08";
    String objectName = String.format("%s.txt", scenario);
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, "".getBytes(StandardCharsets.UTF_8));

      logger.info(String.format("[%s] Downloading the file.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectName)
          .withVersionId(response.getVersionId())
          .build();
      Path path = Paths.get(objectName);
      bucketClient.download(downloadRequest, path);
      String actualContent = Files.readString(path);
      if(actualContent.isEmpty()) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_DOWNLOAD_09() {
    String scenario = "GCS_DOWNLOAD_09";
    String objectName = String.format("%s-_.]@ .txt", scenario);
    String expectedContent = "Hello World!";
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, expectedContent.getBytes(StandardCharsets.UTF_8));

      logger.info(String.format("[%s] Downloading the file.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectName)
          .withVersionId(response.getVersionId())
          .build();
      Path path = Paths.get(objectName);
      bucketClient.download(downloadRequest, path);
      String actualContent = Files.readString(path);
      if(expectedContent.equals(actualContent)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_DOWNLOAD_10() {
    String scenario = "GCS_DOWNLOAD_10";
    String objectName = String.format("%s.txt", scenario);
    String expectedContent1 = "Hello World - first Version";
    String expectedContent2 = "Hello World - second Version";
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(String.format("temp/%s",objectName))
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response1 = bucketClient.upload(uploadRequest, expectedContent1.getBytes(StandardCharsets.UTF_8));
      UploadResponse response2 = bucketClient.upload(uploadRequest, expectedContent2.getBytes(StandardCharsets.UTF_8));

      logger.info(String.format("[%s] Downloading the file.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(String.format("temp/%s",objectName))
          .withVersionId(response1.getVersionId())
          .build();
      Path path = Paths.get(objectName);
      DownloadResponse dr = bucketClient.download(downloadRequest, path);
      String actualContent1 = Files.readString(path);

      if(expectedContent1.equals(actualContent1)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
    finally {
      deleteFile(objectName);
    }
  }

  public static void GCS_DOWNLOAD_11() {
    String scenario = "GCS_DOWNLOAD_11";
    String objectKey = String.format("%s.txt", scenario);
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      String content = generateLargeRandomString(10);
      UploadResponse response = bucketClient.upload(uploadRequest, content.getBytes());

      logger.info(String.format("[%s] Downloading the object using input stream.", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectKey)
          .build();

      DownloadResponse dr = bucketClient.download(downloadRequest);
      InputStream inputStream = dr.getInputStream();
      StringBuilder actualContent = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;

        while ((line = reader.readLine()) != null) {
          actualContent.append(line);
          actualContent.append(System.lineSeparator());
        }

      } catch (IOException e) {
        e.printStackTrace();
      }

      if(content.equals(actualContent)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }
  public static void GCS_DELETE_01() {
    String scenario = "GCS_DELETE_01";
    String objectName = String.format("temp/%s",String.format("%s.txt", scenario));
    String content = "Hello World";
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, content.getBytes(StandardCharsets.UTF_8));

      logger.info(String.format("[%s] Deleting the file.", scenario));
      bucketClient.delete(objectName, null);
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_DELETE_02() {
    String scenario = "GCS_DELETE_02";
    String objectName = String.format("temp/%s",String.format("%s.txt", scenario));
    String content1 = "Hello World - first version";
    String content2 = "Hello World - second version";
    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response1 = bucketClient.upload(uploadRequest, content1.getBytes(StandardCharsets.UTF_8));
      UploadResponse response2 = bucketClient.upload(uploadRequest, content2.getBytes(StandardCharsets.UTF_8));

      logger.info(String.format("[%s] Deleting the file.", scenario));
      bucketClient.delete(objectName, response1.getVersionId());
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_DELETE_03() {
    String scenario = "GCS_DELETE_03";
    String objectName01 = String.format("temp/%s",String.format("%s.txt", scenario + "01"));
    String objectName02 = String.format("temp/%s",String.format("%s.txt", scenario + "02"));
    String objectName03 = String.format("temp/%s",String.format("%s.txt", scenario + "03"));
    String objectName04 = String.format("temp/%s",String.format("%s.txt", scenario + "04"));

    String content1 = "Hello World - first version";
    String content2 = "Hello World - second version";
    try {
      UploadRequest uploadRequest01 = new UploadRequest.Builder()
          .withKey(objectName01)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      UploadRequest uploadRequest02 = new UploadRequest.Builder()
          .withKey(objectName02)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      UploadRequest uploadRequest03 = new UploadRequest.Builder()
          .withKey(objectName03)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      UploadRequest uploadRequest04 = new UploadRequest.Builder()
          .withKey(objectName04)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading file to non-existing object(file)", scenario));
      UploadResponse response01 = bucketClient.upload(uploadRequest01, content1.getBytes(StandardCharsets.UTF_8));
      UploadResponse response02 = bucketClient.upload(uploadRequest02, content2.getBytes(StandardCharsets.UTF_8));
      UploadResponse response03 = bucketClient.upload(uploadRequest03, content2.getBytes(StandardCharsets.UTF_8));
      UploadResponse response04 = bucketClient.upload(uploadRequest04, content2.getBytes(StandardCharsets.UTF_8));

      logger.info(String.format("[%s] Deleting objects.", scenario));
      Collection<BlobIdentifier> toDelete = List.of(
          new BlobIdentifier(objectName01, null),
          new BlobIdentifier(objectName02, null),
          new BlobIdentifier(objectName03, null),
          new BlobIdentifier(objectName04, null)
      );
      bucketClient.delete(toDelete);
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_DELETE_04() {
    String scenario = "GCS_DELETE_04";
    try {
      logger.info(String.format("[%s] Deleting the file.", scenario));
      bucketClient.delete(String.format("temp/%s", scenario), null);
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_COPY_01() {
    String scenario = "GCS_COPY_01";
    String sourceObject = String.format("%s.txt", UUID.randomUUID());
    String targetObject = String.format("%s.txt", UUID.randomUUID());
    String sourceKey = String.format("temp/source/%s",sourceObject);
    String destKey = String.format("temp-01/target/%s",targetObject);

    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(sourceKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, generateLargeString(10).getBytes());

      logger.info("Copying to destination ...");
      CopyRequest copyRequest = CopyRequest.builder().srcKey(sourceKey).destKey(destKey).destBucket(
          bucketClient.getBucket()).build();
      bucketClient.copy(copyRequest);
      logger.info(String.format("[%s] Content copied. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_COPY_02() {
    String scenario = "GCS_COPY_02";
    String sourceObject = String.format("%s.txt", UUID.randomUUID());
    String sourceKey = String.format("temp/source/%s",sourceObject);
    String destKey = String.format("temp-01/target/%s",sourceObject);
    String destinationBucket = "mcj-blob-test-01";

    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(sourceKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, generateLargeString(10).getBytes());

      logger.info("Copying to destination ...");
      CopyRequest copyRequest = CopyRequest.builder().srcKey(sourceKey).destKey(destKey).destBucket(destinationBucket).build();
      bucketClient.copy(copyRequest);
      logger.info(String.format("[%s] Content copied. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_COPY_03() {
    String scenario = "GCS_COPY_03";
    String sourceObject = String.format("%s.txt", UUID.randomUUID());
    String targetObject = String.format("%s.txt", UUID.randomUUID());
    String sourceKey = String.format("temp/source/%s",sourceObject);
    String destKey = String.format("temp-01/target/%s",targetObject);

    try {
      UploadRequest uploadRequest01 = new UploadRequest.Builder()
          .withKey(sourceKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      UploadRequest uploadRequest02 = new UploadRequest.Builder()
          .withKey(destKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response01 = bucketClient.upload(uploadRequest01, "Hello World!".getBytes());
      UploadResponse response02 = bucketClient.upload(uploadRequest02, generateLargeString(10).getBytes());

      logger.info("Copying to destination ...");
      CopyRequest copyRequest = CopyRequest.builder().srcKey(sourceKey).destKey(destKey).destBucket(
          bucketClient.getBucket()).build();
      bucketClient.copy(copyRequest);
      logger.info(String.format("[%s] Content copied. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_METADATA_01() {
    String scenario = "GCS_METADATA_01";
    String objectName = String.format("%s.txt", UUID.randomUUID());

    try {
      UploadRequest uploadRequest = new UploadRequest.Builder()
          .withKey(objectName)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response = bucketClient.upload(uploadRequest, "Hello World!".getBytes());

      logger.info(String.format("[%s] getting metadata", scenario));

      BlobMetadata blobMetadata = bucketClient.getMetadata(objectName, null);

      if(blobMetadata.getMetadata().get(scenario).toString().equals(String.format("%s_VAL", scenario))) {
        logger.info(String.format("[%s] Test PASSED!", scenario));
      }
      else {
        logger.error(String.format("[%s] Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_LIST_OBJECTS_01() {
    String scenario = "GCS_LIST_OBJECTS_01";
    String sourceObject01 = String.format("%s.txt", UUID.randomUUID());
    String sourceObject02 = String.format("%s.txt", UUID.randomUUID());
    String sourceKey01 = String.format("%s/%s", scenario, sourceObject01);
    String sourceKey02 = String.format("%s/%s", scenario, sourceObject02);

    try {
      UploadRequest uploadRequest01 = new UploadRequest.Builder()
          .withKey(sourceKey01)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      UploadRequest uploadRequest02 = new UploadRequest.Builder()
          .withKey(sourceKey02)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response01 = bucketClient.upload(uploadRequest01, "Hello World!".getBytes());
      UploadResponse response02 = bucketClient.upload(uploadRequest02, generateLargeString(10).getBytes());

      logger.info(String.format("[%s] listing objects..", scenario));

      ListBlobsRequest request = new ListBlobsRequest.Builder()
          .withPrefix(String.format("%s/", scenario))
          .build();

      Iterator<BlobInfo> blobInfoIterator = bucketClient.list(request);

      Set<String> expectedObjects = Set.of(sourceKey02, sourceKey01);

      while (blobInfoIterator.hasNext()) {
        BlobInfo blobInfo = blobInfoIterator.next();
        if(!expectedObjects.contains(blobInfo.getKey())) {
          logger.error(String.format("[%s] Test FAILED.", scenario));
        }
      }

      logger.info(String.format("[%s] Test PASSED.", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_PRESIGNED_URL_01() {
    String scenario = "GCS_PRESIGNED_URL_01";
    String sourceObject01 = String.format("%s.txt", UUID.randomUUID());
    String sourceKey01 = String.format("%s/%s", scenario, sourceObject01);

    try {
      UploadRequest uploadRequest01 = new UploadRequest.Builder()
          .withKey(sourceKey01)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response01 = bucketClient.upload(uploadRequest01, "Hello World!".getBytes());

      logger.info(String.format("[%s] generating presigned url..", scenario));

      PresignedUrlRequest requestBuilder = PresignedUrlRequest.builder()
          .key(sourceKey01)
          .type(PresignedOperation.DOWNLOAD)
          .metadata(Map.of("key1", "val1"))
          .duration(Duration.ofMinutes(1))
          .build();
      URL presignedUrl = bucketClient.generatePresignedUrl(requestBuilder);
      logger.info(String.format("[%s] Test PASSED.", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_PRESIGNED_URL_02() {
    String scenario = "GCS_PRESIGNED_URL_02";
    String sourceObject01 = String.format("%s.txt", UUID.randomUUID());
    String sourceKey01 = String.format("%s/%s", scenario, sourceObject01);

    try {
      UploadRequest uploadRequest01 = new UploadRequest.Builder()
          .withKey(sourceKey01)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      UploadResponse response01 = bucketClient.upload(uploadRequest01, "Hello World!".getBytes());

      logger.info(String.format("[%s] generating presigned url..", scenario));

      PresignedUrlRequest requestBuilder = PresignedUrlRequest.builder()
          .key(sourceKey01)
          .type(PresignedOperation.UPLOAD)
          .metadata(Map.of("key1", "val1"))
          .duration(Duration.ofMinutes(1))
          .build();
      URL presignedUrl = bucketClient.generatePresignedUrl(requestBuilder);
      logger.info(String.format("[%s] Test PASSED.", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_MPU_01() {
    String scenario = "GCS_MPU_01";
    String sourceObject = String.format("%s.txt", UUID.randomUUID());
    String objectKey = String.format("%s/%s", scenario, sourceObject);

    try {
      MultipartUploadRequest multipartUploadRequest = new MultipartUploadRequest.Builder()
          .withKey(objectKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      String content01 = generateLargeString(10);
      String content02 = generateLargeString(10);
      String content03 = generateLargeString(10);
      String content04 = generateLargeString(10);

      MultipartUpload multipartUpload = bucketClient.initiateMultipartUpload(multipartUploadRequest);
      MultipartPart multipartPart01 = new MultipartPart(1, content01.getBytes());
      MultipartPart multipartPart02 = new MultipartPart(2, content02.getBytes());
      MultipartPart multipartPart03 = new MultipartPart(3, content03.getBytes());
      MultipartPart multipartPart04 = new MultipartPart(4, content04.getBytes());

      UploadPartResponse uploadPartResponse01 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart01);
      UploadPartResponse uploadPartResponse02 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart02);
      UploadPartResponse uploadPartResponse03 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart03);
      UploadPartResponse uploadPartResponse04 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart04);

      List<UploadPartResponse> parts = List.of(uploadPartResponse01, uploadPartResponse02, uploadPartResponse03, uploadPartResponse04);
      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      bucketClient.completeMultipartUpload(multipartUpload, parts);
      StringBuilder originalContentBld = new StringBuilder();
      originalContentBld.append(content01).append(content02).append(content03).append(content04);
      String expectedContent = originalContentBld.toString();

      logger.info(String.format("[%s] Downloading actual content..", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectKey)
          .build();
      ByteArray downloadedBytes = new ByteArray();
      bucketClient.download(downloadRequest,downloadedBytes);
      byte[] actualBytes = downloadedBytes.getBytes();
      String actualContent = new String(actualBytes);

      if(expectedContent.equals(actualContent)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
      else {
        logger.error(String.format("[%s] Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_MPU_02() {
    String scenario = "GCS_MPU_02";
    String sourceObject = String.format("%s.txt", UUID.randomUUID());
    String objectKey = String.format("%s/%s", scenario, sourceObject);

    try {
      MultipartUploadRequest multipartUploadRequest = new MultipartUploadRequest.Builder()
          .withKey(objectKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      String content01 = generateLargeRandomString(10);
      String content02 = generateLargeRandomString(10);
      String content03 = generateLargeRandomString(10);
      String content04 = generateLargeRandomString(10);

      MultipartUpload multipartUpload = bucketClient.initiateMultipartUpload(multipartUploadRequest);
      MultipartPart multipartPart01 = new MultipartPart(1, content01.getBytes());
      MultipartPart multipartPart02 = new MultipartPart(2, content02.getBytes());
      MultipartPart multipartPart03 = new MultipartPart(3, content03.getBytes());
      MultipartPart multipartPart04 = new MultipartPart(4, content04.getBytes());

      UploadPartResponse uploadPartResponse01 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart01);
      UploadPartResponse uploadPartResponse02 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart02);
      UploadPartResponse uploadPartResponse03 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart03);
      UploadPartResponse uploadPartResponse04 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart04);

      List<UploadPartResponse> parts = List.of(uploadPartResponse01, uploadPartResponse02, uploadPartResponse03, uploadPartResponse04);
      logger.info(String.format("[%s] Uploading content to non-existing object(file)", scenario));
      bucketClient.completeMultipartUpload(multipartUpload, parts);

      //repeat upload to replace content with new content
      String newContent01 = generateLargeRandomString(10);
      String newContent02 = generateLargeRandomString(10);
      String newContent03 = generateLargeRandomString(10);
      String newContent04 = generateLargeRandomString(10);

      MultipartUpload newMultipartUpload = bucketClient.initiateMultipartUpload(multipartUploadRequest);
      MultipartPart newMultipartPart01 = new MultipartPart(1, newContent01.getBytes());
      MultipartPart newMultipartPart02 = new MultipartPart(2, newContent02.getBytes());
      MultipartPart newMultipartPart03 = new MultipartPart(3, newContent03.getBytes());
      MultipartPart newMultipartPart04 = new MultipartPart(4, newContent04.getBytes());

      UploadPartResponse newUploadPartResponse01 = bucketClient.uploadMultipartPart(newMultipartUpload, newMultipartPart01);
      UploadPartResponse newUploadPartResponse02 = bucketClient.uploadMultipartPart(newMultipartUpload, newMultipartPart02);
      UploadPartResponse newUploadPartResponse03 = bucketClient.uploadMultipartPart(newMultipartUpload, newMultipartPart03);
      UploadPartResponse newUploadPartResponse04 = bucketClient.uploadMultipartPart(newMultipartUpload, newMultipartPart04);

      List<UploadPartResponse> newParts = List.of(newUploadPartResponse01, newUploadPartResponse02, newUploadPartResponse03, newUploadPartResponse04);
      logger.info(String.format("[%s] Uploading content to existing object(file)", scenario));
      bucketClient.completeMultipartUpload(newMultipartUpload, newParts);

      StringBuilder newContentBld = new StringBuilder();
      newContentBld.append(newContent01).append(newContent02).append(newContent03).append(newContent04);
      String expectedContent = newContentBld.toString();

      logger.info(String.format("[%s] Downloading actual content..", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectKey)
          .build();
      ByteArray downloadedBytes = new ByteArray();
      bucketClient.download(downloadRequest,downloadedBytes);
      byte[] actualBytes = downloadedBytes.getBytes();
      String actualContent = new String(actualBytes);

      if(expectedContent.equals(actualContent)) {
        logger.info(String.format("[%s] Test PASSED.", scenario));
      }
      else {
        logger.error(String.format("[%s] Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  public static void GCS_MPU_03() {
    String scenario = "GCS_MPU_03";
    String sourceObject = String.format("%s.txt", UUID.randomUUID());
    String objectKey = String.format("%s/%s", scenario, sourceObject);

    try {
      MultipartUploadRequest multipartUploadRequest = new MultipartUploadRequest.Builder()
          .withKey(objectKey)
          .withMetadata(Map.of(scenario, String.format("%s_VAL", scenario)))
          .build();

      String content01 = generateLargeString(10);
      String content02 = generateLargeString(10);
      String content03 = generateLargeString(10);
      String content04 = generateLargeString(10);

      MultipartUpload multipartUpload = bucketClient.initiateMultipartUpload(multipartUploadRequest);
      MultipartPart multipartPart01 = new MultipartPart(1, content01.getBytes());
      MultipartPart multipartPart02 = new MultipartPart(2, content02.getBytes());
      MultipartPart multipartPart03 = new MultipartPart(3, content03.getBytes());
      MultipartPart multipartPart04 = new MultipartPart(4, content04.getBytes());

      UploadPartResponse uploadPartResponse01 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart01);
      UploadPartResponse uploadPartResponse02 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart02);
      UploadPartResponse uploadPartResponse03 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart03);
      UploadPartResponse uploadPartResponse04 = bucketClient.uploadMultipartPart(multipartUpload, multipartPart04);

      List<UploadPartResponse> parts = List.of(uploadPartResponse01, uploadPartResponse02, uploadPartResponse03, uploadPartResponse04);
      logger.info(String.format("[%s] Aborting upload...", scenario));

      bucketClient.abortMultipartUpload(multipartUpload);

      logger.info(String.format("[%s] Downloading actual content..", scenario));
      DownloadRequest downloadRequest = new DownloadRequest.Builder()
          .withKey(objectKey)
          .build();
      ByteArray downloadedBytes = new ByteArray();
      bucketClient.download(downloadRequest,downloadedBytes);
      logger.error(String.format("[%s] Test FAILED.", scenario));
    } catch (SubstrateSdkException e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.info(String.format("[%s] Exception verified. Test PASSED.", scenario));
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

  public static String generateLargeRandomString(int sizeInMB) {
    // Convert MB to characters. 1 MB = 1,024 * 1,024 bytes. Assuming 1 character = 1 byte (for ASCII).
    long sizeInBytes = (long) sizeInMB * 1024 * 1024;

    // Use a StringBuilder for efficient string concatenation
    StringBuilder sb = new StringBuilder();

    // Generate and append random characters until the desired size is reached
    for (long i = 0; i < sizeInBytes; i++) {
      int randomIndex = RANDOM.nextInt(CHARACTERS.length());
      sb.append(CHARACTERS.charAt(randomIndex));
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