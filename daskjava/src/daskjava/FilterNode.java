package daskjava;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FilterNode extends TaskNode {
    private final TaskNode parent;
    private final Predicate<Map<String, String>> predicate;

    public FilterNode(TaskNode parent, Predicate<Map<String, String>> predicate) {
        this.parent = parent;
        this.predicate = predicate;
    }

    @Override
    public List<Partition> execute() {
        List<Partition> parentPartitions = parent.execute();

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(parentPartitions.size(), Runtime.getRuntime().availableProcessors()));
        List<Future<Partition>> futures = new ArrayList<>();
        for (Partition p : parentPartitions) {
            futures.add(executor.submit(() -> {
                List<Map<String, String>> filtered = p.getRows().stream()
                        .filter(predicate)
                        .collect(Collectors.toList());
                return new Partition(filtered);
            }));
        }

        List<Partition> res = new ArrayList<>();
        for (Future<Partition> f : futures) {
            try {
                res.add(f.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
        return res;
    }

    public static void main(String[] args) {
        DaskDataFrame ddf = DaskDataFrame
                .readCSV("/Users/hoonit4/Documents/Projects/JDask/Bakery.csv")
                .filter(row -> row.get("Transaction").equals("5"));
        System.out.println(ddf);
    }
}
