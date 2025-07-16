package sf.mcj.main;

import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sf.mcj.clients.FireStoreMCJClient;
import sf.mcj.main.TestData.CompositeProduct;
import sf.mcj.main.TestData.Item;
import sf.mcj.main.TestData.KitProduct;
import sf.mcj.main.TestData.KitProductDetails;
import sf.mcj.main.TestData.Matrix;
import sf.mcj.main.TestData.Product;
import sf.mcj.main.TestData.ProductDetails;
import sf.mcj.main.TestData.SuperProduct;

public class FireStoreMCJTest {

  private static DocStoreClient docStoreClient;

  private static final Logger logger = LoggerFactory.getLogger(FireStoreMCJClient.class);

  public static void main(String[] args) {
    //****** Please recreate the collection Products before running the test cases*****

    if(!initializeDocStoreClient()) {
      logger.error("Firestore initialization failed. Exiting...");
    }

    logger.info("---- Running Firestore Tests-----");

    FS_IT_CREATE_01();

    FS_IT_CREATE_02();

    FS_IT_CREATE_03();

    FS_IT_CREATE_04();

    FS_IT_CREATE_05();

    FS_IT_CREATE_06();

    FS_IT_GET_01();

    FS_IT_GET_02();

    FS_IT_GET_03();

    FS_IT_GET_04();

    FS_IT_PUT_01();

    FS_IT_PUT_02();

    FS_IT_PUT_03();

    FS_IT_QUERY_01();

    FS_IT_QUERY_02();

    FS_IT_QUERY_03();

    FS_IT_QUERY_04();

    FS_IT_QUERY_06();

    FS_IT_QUERY_07();

    FS_IT_ATOMIC_TRANSACT_01();

    FS_IT_ATOMIC_TRANSACT_02();

    FS_IT_ATOMIC_TRANSACT_03();

    FS_IT_ATOMIC_TRANSACT_04();

    FS_BATCH_WRITE_01();

    closeConnection();
  }

  private static void FS_IT_CREATE_01() {
    String scenario = "FS_IT_CREATE_01";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    Product testData = new Product("comp001", "Deluxe Widget", details, 29.99, true);

    try {
      docStoreClient.create(new Document(testData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      Product retrievedData = Product.builder().id(testData.getId()).build();
      docStoreClient.get(new Document(retrievedData));

        // Basic verification
        if (testData.equals(retrievedData)) {
          logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
        } else {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
        }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_CREATE_02() {
    String scenario = "FS_IT_CREATE_02";

    Map<String, Object> features = new HashMap<>();
    features.put("color", "black");
    features.put("weightKg", 0.5);
    features.put("bluetooth", true);

    Map<String, Integer> dimensions = new HashMap<>();
    dimensions.put("width", 10);
    dimensions.put("height", 5);
    dimensions.put("depth", 2);

    Item testData = new Item("item005", Arrays.asList("electronics", "gadget", "sale"), features, Arrays.asList(4, 5, 3, 5), dimensions);

    try {
      docStoreClient.create(new Document(testData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));

      Item retrievedData = Item.builder().id(testData.getId()).build();
      docStoreClient.get(new Document(retrievedData));
      logger.info(String.format("[%s] Verifying document data...", scenario));

        // Basic verification
        if (testData.equals(retrievedData)) {
          logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
        } else {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED", scenario));
        }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_CREATE_03() {
    String scenario = "FS_IT_CREATE_03";

    List<List<Integer>> dataGrid = new ArrayList<>();
    dataGrid.add(Arrays.asList(1, 2, 3));
    dataGrid.add(Arrays.asList(4, 5, 6));
    dataGrid.add(Arrays.asList(7, 8, 9));

    Map<String, String> meta = new HashMap<>();
    meta.put("source", "test");

    Matrix testData = new Matrix("mat_fail_001", dataGrid, meta);

    try {
      logger.info(String.format("[%s] Attempting to create document (expected to fail)", scenario));

      docStoreClient.create(new Document(testData));
    } catch (InvalidArgumentException ex) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_CREATE_04() {
    String scenario = "FS_IT_CREATE_04";
    // 1. Long Id
    StringBuilder titleBuilder = new StringBuilder();
    for (int i = 0; i < 50000; i++) { // ~50KB (50000 chars * 1 byte/char approx for ASCII)
      titleBuilder.append("This is a very long Id. ");
    }
    String longId = titleBuilder.toString();

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    Product testData = new Product(longId, "Deluxe Widget", details, 29.99, true);

    try {
      logger.info(String.format("[%s] Attempting to create document (expected to fail)", scenario));
      docStoreClient.create(new Document(testData));
    }
    catch (InvalidArgumentException ex) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_CREATE_05() {
    String scenario = "FS_IT_CREATE_05";

    Map<String, Object> features = new HashMap<>();
    for (int i = 0; i < 10000; i++) { // ~200KB (10000 entries * (key_size + value_size) )
      features.put("Feature " + i + ": feature " + UUID.randomUUID().toString(), i + 1);
    }

    Map<String, Integer> dimensions = new HashMap<>();
    for (int i = 0; i < 10000; i++) { // ~200KB (10000 entries * (key_size + value_size) )
      dimensions.put("Dimension " + i + ": feature " + UUID.randomUUID().toString(), i + 1);
    }

    Item testData = new Item("item005", Arrays.asList("electronics", "gadget", "sale"), features, Arrays.asList(4, 5, 3, 5), dimensions);

    try {
      logger.info(String.format("[%s] Attempting to create document (expected to fail)", scenario));
      docStoreClient.create(new Document(testData));
    }
    catch (InvalidArgumentException ex) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_CREATE_06() {
    String scenario = "FS_IT_CREATE_06";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    Product testData = new Product("MalformedId..001/Id", "Deluxe Widget", details, 29.99, true);

    try {
      logger.info(String.format("[%s] Attempting to create document (expected to fail)", scenario));
      docStoreClient.create(new Document(testData));
    }
    catch (InvalidArgumentException ex) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_GET_01() {
    String scenario = "FS_IT_GET_01";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    SuperProduct testData = SuperProduct.builder().id("mapCustomClass001").productName("Deluxe Widget").details(details).price(29.99).isActive(true).isFree(true).build();

    try {
      docStoreClient.create(new Document(testData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      Product retrievedData = Product.builder().id(testData.getId()).build();
      docStoreClient.get(new Document(retrievedData));
      logger.info(String.format("[%s] Verifying document data...", scenario));

      if (retrievedData.equals(testData)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED", scenario));
      }
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_GET_02() {
    String scenario = "FS_IT_GET_02";

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", "wrongFieldsAndCorrectFields001");
    testData.put("products", List.of("mobile", "headphones", "laptop"));

    try {
      docStoreClient.create(new Document(testData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      Map<String, Object> retrievedData = new HashMap<>();
      retrievedData.put("id", testData.get("id"));
      docStoreClient.get(new Document(retrievedData), new String[]{"id", "products", "wrongField"});
      logger.info(String.format("[%s] Verifying document data...", scenario));

      if (retrievedData.equals(testData)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_GET_03() {
    String scenario = "FS_IT_GET_03";

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", "nestedGetFields001");
    testData.put("products", List.of("mobile", "headphones", "laptop"));
    testData.put("metadata", Map.of("height", 21, "width", 22, "length", 12));

    Map<String, Object> expectedData = new HashMap<>();
    expectedData.put("id", "nestedGetFields001");
    expectedData.put("products", List.of("mobile", "headphones", "laptop"));
    expectedData.put("metadata", Map.of("height", 21, "width", 22));

    try {
      docStoreClient.create(new Document(testData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      Map<String, Object> retrievedData = new HashMap<>();
      retrievedData.put("id", testData.get("id"));
      docStoreClient.get(new Document(retrievedData), new String[]{"id", "products", "metadata.height", "metadata.width"});
      logger.info(String.format("[%s] Verifying document data...", scenario));

      if (expectedData.equals(retrievedData)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_GET_04() {
    String scenario = "FS_IT_GET_04";

    try {
      logger.info(String.format("[%s] Retrieving non-existing document (expected to fail)!", scenario));

      Product retrievedData = Product.builder().id("nonExistIdDoc04").build();
      docStoreClient.get(new Document(retrievedData));
      logger.info(String.format("[%s] Verifying document ...", scenario));

      if (Objects.isNull(retrievedData)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_PUT_01() {
    String scenario = "FS_IT_PUT_01";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    Product createData = new Product("putComp001", "Deluxe Widget", details, 29.99, true);
    SuperProduct updateData = SuperProduct.builder().id("putComp001").productName("Deluxe Widget").details(null).price(29.99).isActive(true).isFree(true).build();


    try {
      docStoreClient.create(new Document(createData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      docStoreClient.put(new Document(updateData));
      logger.info(String.format("[%s] Document updated successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      SuperProduct retrievedData = SuperProduct.builder().id(createData.getId()).build();
      docStoreClient.get(new Document(retrievedData));

      // Basic verification
      if (updateData.equals(retrievedData)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_PUT_02() {
    String scenario = "FS_IT_PUT_02";

    Map<String, Object> features = new HashMap<>();
    features.put("color", "black");
    features.put("weightKg", 0.5);
    features.put("bluetooth", true);

    Map<String, Integer> dimensions = new HashMap<>();
    dimensions.put("width", 10);
    dimensions.put("height", 5);
    dimensions.put("depth", 2);

    Item createTestData = new Item("itemCollection005", Arrays.asList("electronics", "gadget", "sale"), features, Arrays.asList(4, 5, 3, 5), dimensions);

    Map<String, Object> featuresUpdated = new HashMap<>();
    features.put("color", "blue");
    features.put("weightKg", 0.6);
    features.put("bluetooth", true);

    Map<String, Integer> dimensionsUpdated = new HashMap<>();
    dimensions.put("width", 11);
    dimensions.put("height", 6);
    dimensions.put("depth", 200);

    Item updateTestData = new Item("itemCollection005", Arrays.asList("non-electronics", "devices"), featuresUpdated, Arrays.asList(4, 5, 3, 5), dimensionsUpdated);

    try {
      docStoreClient.create(new Document(createTestData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      docStoreClient.put(new Document(updateTestData));
      logger.info(String.format("[%s] Document updated successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      Item retrievedData = Item.builder().id(createTestData.getId()).build();
      docStoreClient.get(new Document(retrievedData));

      // Basic verification
      if (updateTestData.equals(retrievedData)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_PUT_03() {
    String scenario = "FS_IT_PUT_03";

    List<List<Integer>> dataGrid = new ArrayList<>();
    dataGrid.add(Arrays.asList(1, 2, 3));
    dataGrid.add(Arrays.asList(4, 5, 6));
    dataGrid.add(Arrays.asList(7, 8, 9));

    Map<String, String> meta = new HashMap<>();
    meta.put("source", "test");

    Matrix createTestData = new Matrix("mat_put_fail_002", null, meta);

    Matrix updateTestData = new Matrix("mat_put_fail_002", dataGrid, meta);

    try {
      docStoreClient.create(new Document(createTestData));
      logger.info(String.format("[%s] Document created successfully!", scenario));
      logger.info(String.format("[%s] Attempting to update document (expected to fail)", scenario));
      docStoreClient.put(new Document(updateTestData));
    } catch (InvalidArgumentException ex) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_QUERY_01() {
    //********* Make sure composite key is created on id and price before running this test case*********
    String scenario = "FS_IT_QUERY_01";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    Product mobile = new Product("mob001", "mobile", details, 29.99, true);

    Product tablet = new Product("tab001", "tablet", details, 54.99, true);

    Product laptop = new Product("lap001", "laptop", details, 300.99, true);

    try {
      docStoreClient.create(new Document(mobile));
      docStoreClient.create(new Document(tablet));
      docStoreClient.create(new Document(laptop));

      logger.info(String.format("[%s] Documents created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      var documentIterator = docStoreClient.query().where("id", FilterOperation.IN, List.of(mobile.getId(), tablet.getId(), laptop.getId())).orderBy("price", true).get();

      List<Product> expectedProducts = List.of(mobile, tablet, laptop);
      int index = 0;
      while (documentIterator.hasNext()) {
        Product retrievedData = new Product();
        documentIterator.next(new Document(retrievedData));
        if (!expectedProducts.get(index++).equals(retrievedData)) {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
          return;
        }
      }

      logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));

    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_QUERY_02() {
    String scenario = "FS_IT_QUERY_02";

    ProductDetails details = new ProductDetails("1.0.2", "Salesforce Devices", new java.util.Date().toString());
    Product mobile = new Product("mob002", "mobile", details, 29.99, true);

    Product tablet = new Product("tab002", "tablet", details, 54.99, true);

    Product laptop = new Product("lap002", "laptop", details, 300.99, true);

    try {
      docStoreClient.create(new Document(mobile));
      docStoreClient.create(new Document(tablet));
      docStoreClient.create(new Document(laptop));

      logger.info(String.format("[%s] Documents created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      var documentIterator = docStoreClient.query().where("details.manufacturer", FilterOperation.EQUAL, details.manufacturer).get();

      List<Product> expectedProducts = List.of(laptop, mobile, tablet);
      int index = 0;
      while (documentIterator.hasNext()) {
        Product retrievedData = new Product();
        documentIterator.next(new Document(retrievedData));
        if (!expectedProducts.get(index++).equals(retrievedData)) {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
          return;
        }
      }

      if(index != expectedProducts.size()){
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
        return;
      }

      logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));

    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_QUERY_03() {
    //********* Make sure composite key is not created on details.manufacturer and price before running this test case*********
    String scenario = "FS_IT_QUERY_03";

    ProductDetails details = new ProductDetails("1.0.2", scenario, new java.util.Date().toString());
    Product mobile = new Product("mob003", "mobile", details, 29.99, true);

    Product tablet = new Product("tab003", "tablet", details, 54.99, true);

    Product laptop = new Product("lap003", "laptop", details, 300.99, true);

    try {
      docStoreClient.create(new Document(mobile));
      docStoreClient.create(new Document(tablet));
      docStoreClient.create(new Document(laptop));

      logger.info(String.format("[%s] Documents created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      var documentIterator = docStoreClient.query().where("details.manufacturer", FilterOperation.EQUAL, scenario).where("price", FilterOperation.GREATER_THAN_OR_EQUAL_TO, mobile.getPrice()).orderBy("price", false).get();

      List<Product> expectedProducts = List.of(laptop, tablet, mobile);
      int index = 0;
      while (documentIterator.hasNext()) {
        Product retrievedData = new Product();
        documentIterator.next(new Document(retrievedData));
        if (!expectedProducts.get(index).equals(retrievedData)) {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
          return;
        }
        index++;
      }

      if(index != expectedProducts.size()){
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
        return;
      }

      logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));

    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_QUERY_04() {
    //********* Make sure composite key is created on productName and price before running this test case*********
    String scenario = "FS_IT_QUERY_04";

    ProductDetails details = new ProductDetails("1.0.2", scenario, new java.util.Date().toString());
    Product proMobile = new Product("mob004", "mobile", details, 59.99, true);

    Product mobile = new Product("mob0041", "mobile", details, 29.99, true);

    Product tablet = new Product("tab004", "tablet", details, 54.99, true);

    Product laptop = new Product("lap004", "laptop", details, 300.99, true);

    try {
      docStoreClient.create(new Document(mobile));
      docStoreClient.create(new Document(proMobile));
      docStoreClient.create(new Document(tablet));
      docStoreClient.create(new Document(laptop));

      logger.info(String.format("[%s] Documents created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      var documentIterator = docStoreClient.query()
          .where("productName", FilterOperation.IN, List.of(mobile.getProductName(), laptop.getProductName(), tablet.getProductName()))
          .where("price", FilterOperation.GREATER_THAN_OR_EQUAL_TO, mobile.getPrice())
          .orderBy("productName", true)
          .orderBy("price", true).get();

      List<Product> expectedProducts = List.of(laptop, mobile, proMobile, tablet);
      int index = 0;
      while (documentIterator.hasNext()) {
        Product retrievedData = new Product();
        documentIterator.next(new Document(retrievedData));
        if (!expectedProducts.get(index).equals(retrievedData)) {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
          return;
        }
        index++;
      }

      if(index != expectedProducts.size()){
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
        return;
      }

      logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));

    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_QUERY_06() {
    String scenario = "FS_IT_QUERY_06";

    ProductDetails details = new ProductDetails("1.0.2", scenario, new java.util.Date().toString());

    Product mobile = new Product("mob006", "mobile", details, 59.99, true);

    Product tablet = new Product("tab006", "tablet", null, 54.99, true);

    Product laptop = new Product("lap006", "laptop", null, 300.99, true);

    try {
      docStoreClient.create(new Document(mobile));
      docStoreClient.create(new Document(tablet));
      docStoreClient.create(new Document(laptop));

      logger.info(String.format("[%s] Documents created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      var documentIterator = docStoreClient.query()
          .where("id", FilterOperation.IN, List.of(mobile.getId(), laptop.getId(), tablet.getId()))
          .where("details", FilterOperation.EQUAL, null).get();


      List<Product> expectedProducts = List.of(laptop, tablet);
      int index = 0;
      while (documentIterator.hasNext()) {
        Product retrievedData = new Product();
        documentIterator.next(new Document(retrievedData));
        if (!expectedProducts.get(index).equals(retrievedData)) {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
          return;
        }
        index++;
      }

      if(index != expectedProducts.size()){
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
        return;
      }

      logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));

    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_QUERY_07() {
    String scenario = "FS_IT_QUERY_07";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());

    KitProductDetails superProductDetails = KitProductDetails.builder().version("1.0.3").manufacturer("Acme Corp").releaseDate(new java.util.Date().toString()).country("US").build();

    Product mobile = new Product("mobNonExistField007", "mobile", details, 59.99, true);

    Product tablet = Product.builder().id("tabNonExistField007").productName("tablet").details(details).price(54.99).isActive(true).build();

    KitProduct laptop = KitProduct.builder().id("lapNonExistField007").productName("tablet").details(superProductDetails).price(54.99).isActive(true).build();

    try {
      docStoreClient.create(new Document(mobile));
      docStoreClient.create(new Document(tablet));
      docStoreClient.create(new Document(laptop));
      logger.info(String.format("[%s] Documents created successfully!", scenario));

      logger.info(String.format("[%s] Verifying document data...", scenario));
      var documentIterator = docStoreClient.query()
          .where("id", FilterOperation.IN, List.of(mobile.getId(), tablet.getId(), laptop.getId()))
          .where("details.country", FilterOperation.EQUAL, "US").get();


      List<KitProduct> expectedProducts = List.of(laptop);
      int index = 0;
      while (documentIterator.hasNext()) {
        KitProduct retrievedData = new KitProduct();
        documentIterator.next(new Document(retrievedData));
        if (!expectedProducts.get(index).equals(retrievedData)) {
          logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
          return;
        }
        index++;
      }

      if(index != expectedProducts.size()){
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
        return;
      }

      logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));

    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_ATOMIC_TRANSACT_01() {
    String scenario = "FS_IT_ATOMIC_TRANSACT_01";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    CompositeProduct createTestData01 = CompositeProduct.builder().id("atomicCompPutGet001").productName("Deluxe Widget").details(details).price(29.99).isActive(true).subProducts(Map.of("product1", "mobile")).build();

    CompositeProduct createTestData02 = CompositeProduct.builder().id("atomicCompPutGet002").productName("Deluxe Widget").details(details).price(29.99).isActive(true).subProducts(Map.of("product1", "mobile", "product2", "laptop")).build();

    CompositeProduct updateTestData01 = CompositeProduct.builder().id("atomicCompPutGet001").productName("Deluxe Widget").details(details).price(59.99).isActive(true).subProducts(Map.of("product1", "laptop")).build();

    CompositeProduct updateTestData02 = CompositeProduct.builder().id("atomicCompPutGet002").productName("Deluxe Widget").details(details).price(69.99).isActive(true).subProducts(Map.of("product1", "mobile", "product2", "tablet")).build();

    try {
      docStoreClient.create(new Document(createTestData01));
      docStoreClient.create(new Document(createTestData02));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      logger.info(String.format("[%s] Running atomic transaction...", scenario));
      Product retrievedData01 = Product.builder().id(createTestData01.getId()).build();
      Product retrievedData02 = Product.builder().id(createTestData02.getId()).build();
      docStoreClient.getActions()
          .put(new Document(updateTestData01))
          .put(new Document(updateTestData02))
          .get(new Document(retrievedData01))
          .get(new Document(retrievedData02))
          .enableAtomicWrites().run();

      // Basic verification
      if (updateTestData01.equals(retrievedData01) && updateTestData02.equals(retrievedData02)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_ATOMIC_TRANSACT_02() {
    String scenario = "FS_IT_ATOMIC_TRANSACT_02";

    Map<String, Object> features = new HashMap<>();
    for (int i = 0; i < 10000; i++) { // ~200KB (10000 entries * (key_size + value_size) )
      features.put("Feature " + i + ": feature " + UUID.randomUUID().toString(), i + 1);
    }

    Map<String, Integer> dimensions = new HashMap<>();
    for (int i = 0; i < 10000; i++) { // ~200KB (10000 entries * (key_size + value_size) )
      dimensions.put("Dimension " + i + ": feature " + UUID.randomUUID().toString(), i + 1);
    }

    Item testData = new Item("atomicMaxDocPutGet001", Arrays.asList("electronics", "gadget", "sale"), null, Arrays.asList(4, 5, 3, 5), null);
    Item updateTestData = new Item("atomicMaxDocPutGet001", Arrays.asList("electronics", "gadget", "sale"), features, Arrays.asList(4, 5, 3, 5), dimensions);

    try {
      docStoreClient.create(new Document(testData));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      logger.info(String.format("[%s] Running atomic transaction (expected to be failed)...", scenario));
      Product retrievedData01 = Product.builder().id(testData.getId()).build();
      docStoreClient.getActions()
          .put(new Document(updateTestData))
          .get(new Document(retrievedData01))
          .enableAtomicWrites().run();

      // Basic verification
      if (testData.equals(retrievedData01)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
      }
    } catch (InvalidArgumentException e) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    }
    catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_ATOMIC_TRANSACT_03() {
    String scenario = "FS_IT_ATOMIC_TRANSACT_03";

    try {
      docStoreClient.getActions()
          .enableAtomicWrites().run();
      logger.info(String.format("[%s] Operation verified. Test PASSED!", scenario));

    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_IT_ATOMIC_TRANSACT_04() {
    String scenario = "FS_IT_ATOMIC_TRANSACT_04";

    ProductDetails details = new ProductDetails("1.0.2", "Acme Corp", new java.util.Date().toString());
    Product createTestData01 = Product.builder().id("atomicMultiplePutGet001").productName("Deluxe Widget001").details(details).price(29.99).isActive(true).build();

    Product createTestData02 = Product.builder().id("atomicMultiplePutGet002").productName("Deluxe Widget002").details(details).price(29.99).isActive(true).build();

    Product createTestData03 = Product.builder().id("atomicMultiplePutGet003").productName("Deluxe Widget003").details(details).price(59.99).isActive(true).build();

    Product createTestData04 = Product.builder().id("atomicMultiplePutGet004").productName("Deluxe Widget004").details(details).price(69.99).isActive(true).build();

    Product updateTestData01 = Product.builder().id("atomicMultiplePutGet001").productName("Deluxe Widget003").details(details).price(59.99).isActive(true).build();

    Product updateTestData02 = Product.builder().id("atomicMultiplePutGet002").productName("Deluxe Widget004").details(details).price(69.99).isActive(true).build();

    try {
      docStoreClient.create(new Document(createTestData01));
      docStoreClient.create(new Document(createTestData02));
      docStoreClient.create(new Document(createTestData03));
      docStoreClient.create(new Document(createTestData04));
      logger.info(String.format("[%s] Document created successfully!", scenario));

      logger.info(String.format("[%s] Running atomic transaction...", scenario));
      Product retrievedData01 = Product.builder().id(createTestData01.getId()).build();
      Product retrievedData02 = Product.builder().id(createTestData02.getId()).build();
      Product retrievedData03 = Product.builder().id(createTestData03.getId()).build();
      Product retrievedData04 = Product.builder().id(createTestData04.getId()).build();

      var actions = docStoreClient.getActions()
          .get(new Document(retrievedData01))
          .get(new Document(retrievedData02))
          .get(new Document(retrievedData03))
          .get(new Document(retrievedData04));

      Product updateRetrievedData01 = Product.builder().id(retrievedData01.getId()).productName(retrievedData03.getProductName()).price(retrievedData03.getPrice()).isActive(retrievedData03.isActive()).details(retrievedData03.getDetails()).build();
      Product updateRetrievedData02 = Product.builder().id(retrievedData02.getId()).productName(retrievedData04.getProductName()).price(retrievedData04.getPrice()).isActive(retrievedData04.isActive()).details(retrievedData04.getDetails()).build();

      actions.put(new Document(updateRetrievedData01)).put(new Document(updateRetrievedData02));

      actions.get(new Document(updateRetrievedData01)).get(new Document(updateRetrievedData02));

      actions.enableAtomicWrites().run();

      // Basic verification
      if (updateTestData01.equals(retrievedData01) && updateTestData02.equals(retrievedData02)) {
        logger.info(String.format("[%s] Data integrity verified. Test PASSED!", scenario));
      } else {
        logger.info(String.format("[%s] Data mismatch after retrieval. Test FAILED.", scenario));
      }
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static void FS_BATCH_WRITE_01() {
    String scenario = "FS_BATCH_WRITE_01";
    List<Document> products = new ArrayList<>();

    long targetTotalSizeMB = 10;
    long targetTotalSizeBytes = targetTotalSizeMB * 1024 * 1024;

    long sizePerProductDetailsKB = targetTotalSizeBytes / 10 / 1024;

    for (int i = 1; i <= 20; i++) {
      String id = "batchRollbackTest00" + i;
      String productName = "Mega Gadget Pro " + i;
      double price = 999.99 + i;
      boolean isActive = i % 2 == 0;

      // a large string for ProductDetails
      String veryLongData = generateLargeString(sizePerProductDetailsKB * 1024);

      ProductDetails details = new ProductDetails("1.0.2", veryLongData, new java.util.Date().toString());

      Product product = Product.builder()
          .id(id)
          .productName(productName)
          .details(details)
          .price(price)
          .isActive(isActive)
          .build();

      products.add(new Document(product));
    }

    try {
      logger.info(String.format("[%s] Creating documents (expected to fail) ....!", scenario));
      docStoreClient.batchPut(products);
    } catch (DeadlineExceededException e) {
      logger.info(String.format("[%s] Exception verified. Test PASSED!", scenario));
    } catch (Exception e) {
      logger.error(String.format("[%s] Error during test: %s", scenario, e.getMessage()));
      e.printStackTrace();
      logger.error(String.format("[%s] Test FAILED.", scenario));
    }
  }

  private static String generateLargeString(long sizeInBytes) {
    // Use StringBuilder for efficient string concatenation
    StringBuilder sb = new StringBuilder((int) sizeInBytes);
    // Append characters until the desired size is approximately reached.
    // Assuming 1 character = 1 byte for simplicity (e.g., ASCII/Latin-1 characters).
    // In Java, String uses char[], which are UTF-16, so each char is 2 bytes.
    // To get `sizeInBytes` in total, we need `sizeInBytes / 2` characters.
    for (long i = 0; i < sizeInBytes / 2; i++) {
      sb.append('A'); // Append a simple character
    }
    return sb.toString();
  }
    private static boolean initializeDocStoreClient () {
      try {
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("projects/svc-prj-test-2/databases/sf-mcj-test-fs-db/documents/Products")
            .withPartitionKey("id")
            //.withSortKey("publisher")
            //.withRevisionField("docRevision")
            .withAllowScans(true)
            .build();

        var docStoreClientBuilder = DocStoreClient.builder("gcp-firestore")
            .withRegion("us-west-2")
            .withCollectionOptions(collectionOptions);

        docStoreClient = docStoreClientBuilder.build();
        return true;
      } catch (SubstrateSdkException sse) {
        logger.error("FireStore connection failed. SDK caught exception raised with error"
            + sse.getMessage());
        return false;
      } catch (Exception ex) {
        logger.error("FireStore connection failed. SDK uncaught exception raised with error"
            + ex.getMessage());
        return false;
      }
    }

    private static void closeConnection () {
      if (docStoreClient != null) {
        docStoreClient.close();
      }
    }
  }