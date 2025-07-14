package sf.mcj.main.TestData;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Builder
public class Item {

  public String id;
  public List<String> tags;
  public Map<String, Object> features;
  public List<Integer> ratings;
  public Map<String, Integer> dimensions;

  @Override
  public boolean equals(Object o) {
    // Check if the objects are the same instance
    if (this == o) return true;
    // Check if the other object is null or not of the same class
    if (o == null || getClass() != o.getClass()) return false;
    // Cast the object to Item
    Item item = (Item) o;
    // Use Objects.equals for all reference types, including List and Map.
    // Objects.equals handles nulls gracefully and calls the respective equals methods
    // for collections and maps, providing a deep comparison.
    return Objects.equals(id, item.id) &&
        Objects.equals(tags, item.tags) &&
        Objects.equals(features, item.features) &&
        Objects.equals(ratings, item.ratings) &&
        Objects.equals(dimensions, item.dimensions);
  }

  @Override
  public String toString() {
    return "Item{" +
        "itemId='" + id + '\'' +
        ", tags=" + tags +
        ", features=" + features +
        ", ratings=" + ratings +
        ", dimensions=" + dimensions +
        '}';
  }
}
