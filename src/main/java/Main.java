import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    private record Entry(String value, long expiresAt){}
    private static final Map<String, Entry> store = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");
        int port = 6379;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (clientSocket) {
            BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
            OutputStream out = clientSocket.getOutputStream();
            List<String> command;

            while ((command = readCommand(in)) != null) {
                String name = command.get(0);

                if (name.equalsIgnoreCase("PING")) {
                    out.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
                } else if (name.equalsIgnoreCase("ECHO")) {
                    byte[] arg = command.get(1).getBytes(StandardCharsets.UTF_8);
                    out.write(("$" + arg.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                    out.write(arg);
                    out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                } else if (name.equalsIgnoreCase("SET")) {
                    String key = command.get(1);
                    String value = command.get(2);
                    store.put(key, new Entry(value, Long.MAX_VALUE));
                    out.write("+OK\r\n".getBytes(StandardCharsets.UTF_8));
                } else if (name.equalsIgnoreCase("GET")) {
                    Entry entry = store.get(command.get(1));

                    if (entry == null || System.currentTimeMillis() >= entry.expiresAt()) {
                        out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
                    } else {
                        byte[] bytes = entry.value().getBytes(StandardCharsets.UTF_8);
                        out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.write(bytes);
                        out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                    }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b;

        while ((b = in.read()) != -1) {
            if (b == '\n') {
                break;
            }

            if (b != '\r') {
                line.write(b);
            }
        }

        if (b == -1 && line.size() == 0) {
            return null;
        }

        return line.toString(StandardCharsets.UTF_8);
    }

    private static List<String> readCommand(InputStream in) throws IOException {
        String header = readLine(in);

        if (header == null) {
            return null;
        }

        int count = Integer.parseInt(header.substring(1));
        List<String> args = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            String lenLine = readLine(in);
            int len = Integer.parseInt(lenLine.substring(1));
            byte[] data = in.readNBytes(len);
            in.readNBytes(2);
            args.add(new String(data, StandardCharsets.UTF_8));
        }

        return args;
    }
}