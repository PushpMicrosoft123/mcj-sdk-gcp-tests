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
    if (o == null || getClass() != o.getClass()) return false;
    Item item = (Item) o;

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
