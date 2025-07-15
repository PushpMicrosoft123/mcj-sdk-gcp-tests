package sf.mcj.main.TestData;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@Getter
public class SuperProduct extends Product {
    private boolean isFree;

    @Override
    public boolean equals(Object o) {
        // 1. Check if the objects are the same instance
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SuperProduct that = (SuperProduct) o;

        if (!super.equals(o)) return false;
        return isFree == that.isFree;
    }
}
