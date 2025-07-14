package sf.mcj.main.TestData;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProductDetails {

  public String version;
  public String manufacturer;
  public String releaseDate;

  @Override
  public boolean equals(Object o) {
    // Check if the objects are the same instance
    if (this == o) return true;
    // Check if the other object is null or not of the same class
    if (o == null || getClass() != o.getClass()) return false;
    // Cast the object to ProductDetails
    ProductDetails that = (ProductDetails) o;
    // Use Objects.equals for nullable reference types
    return Objects.equals(version, that.version) &&
        Objects.equals(manufacturer, that.manufacturer) &&
        Objects.equals(releaseDate, that.releaseDate);
  }

  @Override
  public String toString() {
    return "ProductDetails{" +
        "version='" + version + '\'' +
        ", manufacturer='" + manufacturer + '\'' +
        ", releaseDate='" + releaseDate + '\'' +
        '}';
  }
}
