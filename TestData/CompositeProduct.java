package sf.mcj.main.TestData;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.Map;
import lombok.experimental.SuperBuilder;

@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Getter
public class CompositeProduct extends Product {
   private Map<String, String> subProducts;

   @Override
   public boolean equals(Object o) {
      // 1. Check if the objects are the same instance
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      
      CompositeProduct that = (CompositeProduct) o;

      if (!super.equals(o)) return false;

      return Objects.equals(subProducts, that.subProducts);
   }
}
