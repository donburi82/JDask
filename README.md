# JDask: A minimal Dask-inspired DataFrame Engine in Java

This project implements a simplified, single-machine, Dask-like system in Java that supports **parallelized, lazy execution** of DataFrame operations. It includes a functional **Notebook-style GUI and backend server** for interactive code execution.

---

## Features

### 1. DaskDataFrame Core Design
- **Partitioned Data**: Data is internally split into multiple partitions (`Partition` class) to enable parallel processing.
- **Lazy Execution**: All operations are modeled as nodes in a task graph (`TaskNode`), and only computed when `execute()` is called.
- **Composable DAG**: APIs like `filter`, `map`, and `groupBy` return new `DaskDataFrame` objects linked to prior nodes.

> Note: A `numPartitions` parameter is maintained and propagated throughout the DAG, but it is not yet dynamically tuned or used for repartitioning.

### 2. Core DataFrame APIs (with lazy, parallel execution)
- `readCSV(path)`: Lazily loads and partitions a CSV file.
- `head(n)`: Retrieves the first `n` rows across partitions.
- `filter(row -> ...)`: Filters rows using a user-defined predicate. Supports simple inline lambdas.
- `map(row -> ...)`: Transforms rows using a user-defined function. Currently supports pre-defined function `addTransaction100`.
- `groupBy(col, aggFunc)`: Aggregates grouped data across partitions. Currently supports `count`.

> Note: `filter`, `map`, and `groupBy` are implemented using `ExecutorService` for parallel execution across partitions.

### 3. Interactive Notebook GUI + Server
- Users can enter code strings similar to:
  ```java
  readCSV("data.csv").filter(row -> row.get("Transaction").equals("5")).head(3)
  ```

## Getting Started

### 1. Run the backend server
Open and run NotebookServer.java. This will start a server on localhost:8000.

### 2. Launch the Notebook UI
Open and run NotebookGUI.java.

### 3. Type and execute code
Type valid code strings in the notebook interface, such as:

```java
readCSV("Bakery.csv")
```

```java
readCSV("Bakery.csv").head(5)
```

```java
readCSV("Bakery.csv").groupBy("Transaction", "count").head(10)
```

```java
// Filter currently only supports row -> row.get("Field").equals("Value") 
readCSV("Bakery.csv").filter(row -> row.get("Transaction").equals("5"))
```

```java
// Map currently only supports "addTransaction100"
readCSV("Bakery.csv").map(addTransaction100)
```

You can execute the code by clicking on "Run" or keyboard-inputting Shift+Enter.

### 4. See results
Output is shown directly below the code cell, or confirmation if no data is returned.

## Components

### 1. Java-style OOP Task Graph
Each DataFrame operation (readCSV, filter, map, groupBy, etc.) is implemented as a subclass of TaskNode, forming a composable and lazily evaluated DAG (Directed Acyclic Graph). This promotes clear separation of concerns and extensibility.

### 2. Multithreading with ExecutorService
Operations like filter, map, and groupBy are executed in parallel across partitions using Java’s ExecutorService, allowing for efficient concurrent computation on a single machine.

### 3. GUI: Interactive Notebook
A Swing-based notebook interface allows users to type in and submit Dask-like chained code expressions (e.g., readCSV(...).filter(...).head(n)). Each cell executes independently and displays its result below the code block.

### 4. Networking: Socket-based Master-Worker Architecture
The GUI acts as the client (master) and connects via TCP socket to a server (worker). User code is sent as a string over the socket, parsed and evaluated server-side, and results are streamed back and displayed in the GUI.

## Future Improvements

### 1. Variable Support
Each cell currently executes a complete one-liner chain. There's no support for variable assignment or reuse between cells. This is also why execute() is called implicitly on every cell.

### 2. Evaluator Logic
The evaluation engine is a naive parser. Filter expressions must follow strict format. Map functions are hardcoded and selected by name.

### 3. Aggregate Function Extensions
Only .groupBy(..., "count") is supported. Extending to sum, avg, min, max, etc., is straightforward.

### 4. Data Type Handling
All cell values are treated as strings. There's no typed schema, parsing logic, or conversion between numeric and string fields.

### 5. Partition Management
While numPartitions is tracked and propagated, there’s currently no way to repartition or dynamically adjust it based on dataset size or CPU cores.
