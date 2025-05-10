package daskjava;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class GroupByNode extends TaskNode {
    private final TaskNode parent;
    private final String groupByColumn;
    private final String aggFunc;
    // needed to extend to other aggregate functions
//    private final String aggColumn;

    public GroupByNode(TaskNode parent, String groupByColumn, String aggFunc) {
        this.parent = parent;
        this.groupByColumn = groupByColumn;
        this.aggFunc = aggFunc;
    }

    @Override
    public List<Partition> execute() {
        List<Partition> parentPartitions = parent.execute();
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(parentPartitions.size(), Runtime.getRuntime().availableProcessors()));

        // Group
        List<Future<Map<String, List<Map<String, String>>>>> futures = new ArrayList<>();
        for (Partition p : parentPartitions) {
            futures.add(executor.submit(() -> p.getRows().stream()
                    .filter(row -> row.containsKey(groupByColumn))
                    .collect(Collectors.groupingBy(row -> row.get(groupByColumn)))
            ));
        }

        Map<String, List<Map<String, String>>> mergedGroups = new HashMap<>();
        for (Future<Map<String, List<Map<String, String>>>> f : futures) {
            try {
                Map<String, List<Map<String, String>>> partGroup = f.get();
                for (var entry : partGroup.entrySet()) {
                    mergedGroups
                            .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                            .addAll(entry.getValue());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();

        // Agg
        List<Map<String, String>> aggRows = new ArrayList<>();
        for (var entry : mergedGroups.entrySet()) {
            String key = entry.getKey();
            List<Map<String, String>> rows = entry.getValue();

            Map<String, String> out = new HashMap<>();
            out.put(groupByColumn, key);

            switch (aggFunc) {
                case "count" -> out.put("count", String.valueOf(rows.size()));
                // should further extend aggregate functions
                default -> out.put("error", "Unknown aggFunc: " + aggFunc);
            }

            aggRows.add(out);
        }

        return List.of(new Partition(aggRows));
    }

    public static void main(String[] args) {
        DaskDataFrame ddf = DaskDataFrame
                .readCSV("/Users/hoonit4/Documents/Projects/JDask/Bakery.csv")
                .groupBy("Transaction", "count")
                .head(10);

        System.out.println(ddf);
    }
}
