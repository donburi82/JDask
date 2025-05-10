package daskjava;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NotebookServer {
    public static void main(String[] args) throws IOException {
        int port = 8000;
        System.out.println("NotebookServer listening on port " + port);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket client = serverSocket.accept();
                new Thread(() -> handleClient(client)).start();
            }
        }
    }

    private static void handleClient(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            // Read until “END”
            StringBuilder codeBuffer = new StringBuilder();
            String line;
            while ((line = in.readLine())!=null) {
                if ("END".equals(line)) {
                    break;
                }
                codeBuffer.append(line).append("\n");
            }
            String code = codeBuffer.toString();
            if (code.isEmpty()) {
                out.println(" No code received.");
                return;
            }

            // Evaluate
            String result;
            try {
                result = evaluate(code);
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                result = "Error during evaluation:\n" + sw;
            }

            // Send back
            out.println(result);
        } catch (IOException e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }

    private static String evaluate(String code) throws Exception {
        DaskDataFrame ddf;

        // readCSV
        if (!code.startsWith("readCSV")) {
            return "Code must start with readCSV";
        }

        int p0 = code.indexOf("(");
        int p1 = code.indexOf(")", p0);
        if (p0<0 || p1<0) {
            return "Malformed readCSV";
        }

        String path = code.substring(p0+1, p1).trim().replaceAll("^\"|\"$", "");
        ddf = DaskDataFrame.readCSV(path);

        // Chained APIs
        String rest = code.substring(p1+1).trim();
        while (!rest.isEmpty()) {
            if (rest.startsWith(".filter(")) {
                int s = rest.indexOf('(');
                int e = findMatchingParentheses(rest, s);
                if (s<0 || e<0) {
                    return "Malformed filter";
                }

                String arg = rest.substring(s + 1, e).trim();
                if (arg.startsWith("row ->")) {
                    // match format: row -> row.get("Field").equals("Value")
                    String pattern = "row\\s*->\\s*row\\.get\\s*\\(\\s*\"([^\"]+)\"\\s*\\)\\s*\\.equals\\s*\\(\\s*\"([^\"]+)\"\\s*\\)";
                    Matcher matcher = Pattern.compile(pattern).matcher(arg);
                    if (matcher.matches()) {
                        String key = matcher.group(1);
                        String value = matcher.group(2);
                        ddf = ddf.filter(row -> {
                            String v = row.get(key);
                            return v != null && v.equals(value);
                        });
                    } else {
                        return "Unsupported inline filter format: " + arg;
                    }
                } else {
                    return "Only inline lambdas supported for filter()";
                }

                rest = rest.substring(e+1).trim();
            } else if (rest.startsWith(".map(")) {
                int s = rest.indexOf('(');
                int e = rest.indexOf(')', s);
                String arg = rest.substring(s+1, e).trim();

                if (arg.equals("addTransaction100")) {
                    ddf = ddf.map(row -> {
                        String tx = row.get("Transaction");
                        if (tx!=null) {
                            try {
                                int newTx = Integer.parseInt(tx)+100;
                                row.put("Transaction+100", String.valueOf(newTx));
                            } catch (NumberFormatException ignored) {}
                        }
                        return row;
                    });
                } else {
                    return "Unsupported map function: " + arg;
                }

                rest = rest.substring(e+1).trim();
            } else if (rest.startsWith(".groupBy(")) {
                int s = rest.indexOf('(');
                int e = rest.indexOf(')', s);
                String[] args = rest.substring(s+1, e).split(",");
                if (args.length==2) {
                    String groupByCol = args[0].trim().replaceAll("^\"|\"$", "");
                    String aggFunc = args[1].trim().replaceAll("^\"|\"$", "");
                    ddf = ddf.groupBy(groupByCol, aggFunc);
                } else {
                    return "groupBy(...) must have exactly 2 arguments";
                }

                rest = rest.substring(e+1).trim();
            } else if (rest.startsWith(".head(")) {
                int s = rest.indexOf('(');
                int e = rest.indexOf(')', s);
                int n = Integer.parseInt(rest.substring(s+1, e).trim());
                ddf = ddf.head(n);

                rest = rest.substring(e+1).trim();
            } else {
                return "Unknown or unsupported operation: " + rest;
            }
        }

        // Execute and return result
        List<Map<String, String>> rows = ddf.execute();
        if (rows.isEmpty()) {
            return "(no rows)";
        }

        StringBuilder sb = new StringBuilder();
        int limit = Math.min(rows.size(), 10);
        for (int i=0; i<limit; ++i) {
            sb.append(rows.get(i)).append("\n");
        }
        if (rows.size()>limit) {
            sb.append("… (").append(rows.size()).append(" total rows)\n");
        }
        return sb.toString();
    }

    private static int findMatchingParentheses(String str, int openIndex) {
        int depth = 0;
        for (int i=openIndex; i<str.length(); ++i) {
            char c = str.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }
}
