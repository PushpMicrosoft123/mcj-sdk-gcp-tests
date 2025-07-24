package sf.mcj.main.TestData;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@Getter
public class Product {
  private String id;
  private String productName;
  private ProductDetails details;
  private double price;
  private boolean isActive;

  @Override
  public boolean equals(Object o) {
    // Check if the objects are the same instance
    if (this == o) return true;
    // Cast the object to Product
    Product product = (Product) o;

    if (Double.compare(product.price, price) != 0) return false;
    if (isActive != product.isActive) return false;
    if (!Objects.equals(id, product.id)) return false;
    if (!Objects.equals(productName, product.productName)) return false;

    return Objects.equals(details, product.details);
  }

  @Override
  public String toString() {
    return "Product{" +
        "id='" + id + '\'' +
        ", productName='" + productName + '\'' +
        ", details=" + details +
        ", price=" + price +
        ", isActive=" + isActive +
        '}';
  }
}

