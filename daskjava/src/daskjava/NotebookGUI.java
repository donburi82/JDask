package daskjava;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.Socket;

public class NotebookGUI extends JFrame {
    private JPanel cellContainer;
    private JScrollPane scrollPane;

    public NotebookGUI() {
        super("JDask Notebook");
        setLayout(new BorderLayout());

        // Top bar with buttons
        JPanel topBar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton addButton = new JButton("Add Code Cell");
        topBar.add(addButton);
        addButton.addActionListener(e -> addCodeCell());
        add(topBar, BorderLayout.NORTH);

        // Cell container panel
        cellContainer = new JPanel();
        cellContainer.setLayout(new BoxLayout(cellContainer, BoxLayout.Y_AXIS));
        scrollPane = new JScrollPane(cellContainer);
        scrollPane.getVerticalScrollBar().setUnitIncrement(16);
        add(scrollPane, BorderLayout.CENTER);

        // etc.
        setExtendedState(JFrame.MAXIMIZED_BOTH);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setVisible(true);

        addCodeCell();
    }

    private void addCodeCell() {
        JPanel cellPanel = new JPanel(new BorderLayout());
        cellPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        JTextArea inputArea = new JTextArea(5, 80);
        inputArea.setFont(new Font("Monospaced", Font.PLAIN, 14));
        JScrollPane inputScroll = new JScrollPane(inputArea);

        JButton runButton = new JButton("Run");

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(inputScroll, BorderLayout.CENTER);
        topPanel.add(runButton, BorderLayout.EAST);

        cellPanel.add(topPanel, BorderLayout.NORTH);
        cellContainer.add(cellPanel);
        cellContainer.revalidate();

        // Shift+Enter
        inputArea.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode()==KeyEvent.VK_ENTER && e.isShiftDown()) {
                    e.consume();
                    runCell(inputArea, cellPanel);
                }
            }
        });

        // Button
        runButton.addActionListener(e -> runCell(inputArea, cellPanel));

        // Scroll to bottom
        SwingUtilities.invokeLater(() -> {
            inputArea.requestFocus();
            scrollPane.getVerticalScrollBar().setValue(scrollPane.getVerticalScrollBar().getMaximum());
        });
    }

    private void runCell(JTextArea inputArea, JPanel cellPanel) {
        String userCode = inputArea.getText().trim();
        if (userCode.isEmpty()) {
            return;
        }

        JTextArea outputArea = new JTextArea();
        outputArea.setEditable(false);
        outputArea.setFont(new Font("Monospaced", Font.PLAIN, 13));
        outputArea.setBackground(new Color(245, 245, 245));
        outputArea.setBorder(BorderFactory.createLineBorder(Color.LIGHT_GRAY));

        try (Socket socket = new Socket("localhost", 8000);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send user code
            out.println(userCode);
            out.println("END");

            // Receive output
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                response.append(line).append("\n");
            }
            outputArea.setText(response.toString().trim().isEmpty() ? "Code executed." : response.toString());
        } catch (IOException ex) {
            outputArea.setText("Error: " + ex.getMessage());
            ex.printStackTrace();
        }

        cellPanel.add(outputArea, BorderLayout.CENTER);
        cellPanel.revalidate();
        cellPanel.repaint();

        addCodeCell();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(NotebookGUI::new);
    }
}
