package daskjava;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MapNode extends TaskNode {
    private final TaskNode parent;
    private final Function<Map<String, String>, Map<String, String>> func;

    public MapNode(TaskNode parent, Function<Map<String, String>, Map<String, String>> func) {
        this.parent = parent;
        this.func = func;
    }

    @Override
    public List<Partition> execute() {
        List<Partition> parentPartitions = parent.execute();
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(parentPartitions.size(), Runtime.getRuntime().availableProcessors()));

        List<Future<Partition>> futures = new ArrayList<>();
        for (Partition p : parentPartitions) {
            futures.add(executor.submit(() -> {
                List<Map<String, String>> mapped = p.getRows().stream()
                        .map(func)
                        .collect(Collectors.toList());
                return new Partition(mapped);
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
                .filter(row -> row.get("Transaction").equals("5"))
                .map(row -> {
                    String tx = row.get("Transaction");
                    if (tx!=null) {
                        try {
                            int newVal = Integer.parseInt(tx)+100;
                            row.put("Transaction+100", String.valueOf(newVal));
                        } catch (NumberFormatException ignored) {}
                    }
                    return row;
                });;

        System.out.println(ddf);
    }
}
