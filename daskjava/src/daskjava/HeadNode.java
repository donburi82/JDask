package daskjava;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HeadNode extends TaskNode {
    private final TaskNode parent;
    private final int n;

    public HeadNode(TaskNode parent, int n) {
        this.parent = parent;
        this.n = n;
    }

    @Override
    public List<Partition> execute() {
        List<Partition> parentPartitions = parent.execute();
        List<Map<String, String>> collected = new ArrayList<>();

        outer:
        for (Partition p : parentPartitions) {
            for (Map<String, String> row : p.getRows()) {
                collected.add(row);
                if (collected.size() == n) break outer;
            }
        }

        List<Partition> result = new ArrayList<>();
        result.add(new Partition(collected));
        return result;
    }

    public static void main(String[] args) {
        DaskDataFrame ddf = DaskDataFrame
                .readCSV("/Users/hoonit4/Documents/Projects/JDask/Bakery.csv")
                .head(5);
        System.out.println(ddf);
    }
}
