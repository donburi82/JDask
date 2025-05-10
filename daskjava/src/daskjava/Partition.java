package daskjava;

import java.util.List;
import java.util.Map;

public class Partition {
    private final List<Map<String, String>> rows;

    public Partition(List<Map<String, String>> rows) {
        this.rows = rows;
    }

    public List<Map<String, String>> getRows() {
        return rows;
    }
}
