package sf.mcj.main.TestData;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Builder
public class Matrix {
  public String id;
  public List<List<Integer>> dataGrid; // Deeply nested array
  public Map<String, String> meta;

  @Override
  public String toString() {
    return "Matrix{" +
        "matrixId='" + id + '\'' +
        ", dataGrid=" + dataGrid +
        ", meta=" + meta +
        '}';
  }
}
