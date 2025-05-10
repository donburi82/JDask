package daskjava;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DaskDataFrame {
    private final TaskNode taskGraph;
    // kind of out of context for now, but important later on (especially when we deal with repartition)
    private final int numPartitions;

    public DaskDataFrame(TaskNode taskGraph, int numPartitions) {
        this.taskGraph = taskGraph;
        this.numPartitions = numPartitions;
    }

    public static DaskDataFrame readCSV(String path) { // entry point
        ReadCSVNode readCSVNode = new ReadCSVNode(path);
        return new DaskDataFrame(readCSVNode, readCSVNode.getNumPartitions());
    }

    public DaskDataFrame head(int n) {
        HeadNode headNode = new HeadNode(this.taskGraph, n);
        return new DaskDataFrame(headNode, this.numPartitions);
    }

    public DaskDataFrame map(Function<Map<String, String>, Map<String, String>> func) {
        MapNode mapNode = new MapNode(this.taskGraph, func);
        return new DaskDataFrame(mapNode, this.numPartitions);
    }

    public DaskDataFrame filter(Predicate<Map<String, String>> predicate) {
        FilterNode filterNode = new FilterNode(this.taskGraph, predicate);
        return new DaskDataFrame(filterNode, this.numPartitions);
    }

    public DaskDataFrame groupBy(String groupByCol, String aggFunc) {
        GroupByNode groupByNode = new GroupByNode(this.taskGraph, groupByCol, aggFunc);
        return new DaskDataFrame(groupByNode, this.numPartitions);
    }

    public List<Map<String, String>> execute() {
        return taskGraph.execute().stream().flatMap(partition -> partition.getRows().stream()).collect(Collectors.toList());
    }

    @Override
    public String toString() { // right now for testing purposes; but shouldn't be this way
        List<Map<String, String>> rows = execute();
        if (rows.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map<String, String> row : rows) {
            sb.append(row.toString()).append("\n");
        }
        return sb.toString();
    }
}
