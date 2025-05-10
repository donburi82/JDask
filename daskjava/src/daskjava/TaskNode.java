package daskjava;

import java.util.List;

public abstract class TaskNode {
    public abstract List<Partition> execute();
}
