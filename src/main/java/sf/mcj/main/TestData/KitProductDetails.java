package sf.mcj.main.TestData;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Getter
public class KitProductDetails extends ProductDetails {
    private String country;

  @Override
  public boolean equals(Object o) {
    // 1. Check if the objects are the same instance
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    KitProductDetails that = (KitProductDetails) o;

    if (!super.equals(o)) return false;
    return Objects.equals(country, that.country);
  }
}
