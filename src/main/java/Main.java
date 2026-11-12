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
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

public class Main {
    private record Entry(Object value, long expiresAt) {}
    private record StreamEntry(String id, Map<String, String> fields){}
    private record RedisStream(List<StreamEntry> entries){}

    private static final Map<String, Entry> store = new ConcurrentHashMap<>();
    private static final Map<String, Queue<BlockingQueue<String>>> waiters = new ConcurrentHashMap<>();

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
                    long expiresAt = Long.MAX_VALUE;

                    if (command.size() >= 5 && command.get(3).equalsIgnoreCase("PX")) {
                        long px = Long.parseLong(command.get(4));
                        expiresAt = System.currentTimeMillis() + px;
                    }

                    store.put(key, new Entry(value, expiresAt));
                    out.write("+OK\r\n".getBytes(StandardCharsets.UTF_8));
                } else if (name.equalsIgnoreCase("GET")) {
                    String key = command.get(1);
                    Entry entry = store.get(key);

                    if (entry != null && System.currentTimeMillis() >= entry.expiresAt()) {
                        store.remove(key, entry);
                        entry = null;
                    }

                    if (entry == null) {
                        out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
                    } else if (entry.value() instanceof String value) {
                        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                        out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.write(bytes);
                        out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                    } else {
                        out.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes(StandardCharsets.UTF_8));
                    }
                } else if (name.equalsIgnoreCase("RPUSH")) {
                    String key = command.get(1);
                    Entry entry = store.get(key);
                    List<String> list;

                    if (entry == null) {
                        list = new ArrayList<>();
                        store.put(key, new Entry(list, Long.MAX_VALUE));
                    }
                    else{
                        list = (List<String>) entry.value();
                    }
                    list.addAll(command.subList(2, command.size()));
                    out.write((":" + list.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

                    Queue<BlockingQueue<String>> line = waiters.get(key);
                    if (line != null) {
                        BlockingQueue<String> tray = line.poll();
                        if (tray != null) {
                            String element = list.remove(0);
                            tray.offer(element);
                        }
                    }
                }
                else if (name.equalsIgnoreCase("LPUSH")) {
                    String key = command.get(1);
                    Entry entry = store.get(key);
                    List<String> list;

                    if(entry == null){
                        list = new ArrayList<>();
                        store.put(key, new Entry(list, Long.MAX_VALUE));
                    }
                    else{
                        list = (List<String>) entry.value();
                    }
                    for(int i = 2; i < command.size(); i++){
                        list.add(0, command.get(i));
                    }
                    out.write((":" + list.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
                }
                else if (name.equalsIgnoreCase("LLEN")) {
                    String key = command.get(1);
                    Entry entry = store.get(key);
                    List<String> list;

                    if(entry == null){
                        out.write(":0\r\n".getBytes(StandardCharsets.UTF_8) );
                    }
                    else{
                        list = (List<String>) entry.value();
                        out.write((":" + list.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
                    }
                }
                else if (name.equalsIgnoreCase("LPOP")) {
                    String key = command.get(1);
                    Entry entry = store.get(key);
                    List<String> list;

                    if(entry == null){
                        out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8) );
                    }
                    else{
                        list = (List<String>) entry.value();

                        if(list.size() == 0){
                            out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8) );
                        }
                        else{
                            if(command.size() == 2){
                                String num = list.remove(0);

                                byte[] bytes = num.getBytes(StandardCharsets.UTF_8);
                                out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                                out.write(bytes);
                                out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                            }
                            else{
                                int count = Integer.parseInt(command.get(2));
                                int n = Math.min(count, list.size());
                                out.write(("*" + n + "\r\n").getBytes(StandardCharsets.UTF_8));

                                for(int i = 0; i < n; i++){
                                    String element = list.remove(0);

                                    byte[] bytes = element.getBytes(StandardCharsets.UTF_8);
                                    out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                                    out.write(bytes);
                                    out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                                }
                            }
                        }
                    }
                }
                else  if(name.equalsIgnoreCase("BLPOP")) {
                    String key = command.get(1);
                    Entry entry = store.get(key);
                    List<String> list = null;

                    if(entry != null){
                        list = (List<String>) entry.value();
                    }

                    if(list != null && list.size() > 0){
                        String element = list.remove(0);
                        out.write(("*2\r\n").getBytes(StandardCharsets.UTF_8));

                        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                        out.write(("$" + keyBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.write(keyBytes);
                        out.write("\r\n".getBytes(StandardCharsets.UTF_8));

                        byte[] bytes = element.getBytes(StandardCharsets.UTF_8);
                        out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.write(bytes);
                        out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                    }
                    else{
                        Queue<BlockingQueue<String>> line =
                                waiters.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>());

                        double seconds = Double.parseDouble(command.get(2));
                        long timeoutMs = (long) (seconds * 1000);

                        BlockingQueue<String> tray = new LinkedBlockingQueue<>();
                        line.add(tray);

                        String element = null;
                        try{
                            if(timeoutMs == 0){
                                element = tray.take();
                            }
                            else{
                                element = tray.poll(timeoutMs, TimeUnit.MILLISECONDS);
                                if(element == null && !line.remove(tray)){
                                    element = tray.take();
                                }
                            }
                        }
                        catch (InterruptedException e){
                            Thread.currentThread().interrupt();
                            return;
                        }

                        if(element == null){
                            out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8) );
                        }
                        else{
                            out.write(("*2\r\n").getBytes(StandardCharsets.UTF_8));

                            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                            out.write(("$" + keyBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                            out.write(keyBytes);
                            out.write("\r\n".getBytes(StandardCharsets.UTF_8));

                            byte[] bytes = element.getBytes(StandardCharsets.UTF_8);
                            out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                            out.write(bytes);
                            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }

                else if (name.equalsIgnoreCase("LRANGE")){
                    String key = command.get(1);
                    int start = Integer.parseInt(command.get(2));
                    int stop = Integer.parseInt(command.get(3));
                    Entry entry = store.get(key);

                    if(entry == null){
                        out.write("*0\r\n".getBytes(StandardCharsets.UTF_8));
                    }
                    else{
                        List<String> list = (List<String>) entry.value();
                        int size = list.size();
                        if(start < 0) start = size - Math.abs(start);
                        if(stop < 0) stop = size - Math.abs(stop);

                        start = Math.max(start, 0);
                        int to = Math.min(stop + 1, size);

                        if(start >= to){
                            out.write("*0\r\n".getBytes(StandardCharsets.UTF_8));
                        }
                        else{
                            List<String> sub = list.subList(start, to);

                            out.write(("*" + sub.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

                            for(String element : sub){
                                byte[] arg = element.getBytes(StandardCharsets.UTF_8);
                                out.write(("$" + arg.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                                out.write(arg);
                                out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    }
                }
                else if(name.equalsIgnoreCase("XADD")){
                    String key = command.get(1);
                    String id = command.get(2);
                    Entry entry = store.get(key);
                    RedisStream stream;

                    if(entry == null){
                        stream = new RedisStream(new ArrayList<>());
                        store.put(key, new Entry(stream, Long.MAX_VALUE));
                    }
                    else{
                        stream = (RedisStream) entry.value();
                    }

                    long lastMs = 0;
                    long lastSeq = 0;

                    if(stream.entries.size() > 0){
                        String lastId = stream.entries.get(stream.entries.size() - 1).id;
                        String[] sp_last = lastId.split("-");

                        lastMs = Long.parseLong(sp_last[0]);
                        lastSeq = Long.parseLong(sp_last[1]);
                    }

                    String[] sp_id;

                    if(id.equals("*")){
                        sp_id = new String[]{String.valueOf(System.currentTimeMillis()), "*"};
                    }
                    else{
                        sp_id = id.split("-");
                    }

                    long newMs = Long.parseLong(sp_id[0]);
                    long newSeq;

                    if(sp_id[1].equals("*")){
                        if(newMs == lastMs){
                            newSeq = lastSeq + 1;
                        }
                        else{
                            newSeq = 0;
                        }
                        id = newMs + "-" + newSeq;
                    }
                    else{
                        newSeq = Long.parseLong(sp_id[1]);
                    }

                    if(newMs == 0 && newSeq == 0){
                        out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes(StandardCharsets.UTF_8));
                        continue;
                    }

                    if(newMs < lastMs || (newMs == lastMs && newSeq <= lastSeq)){
                        out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes(StandardCharsets.UTF_8));
                        continue;
                    }

                    Map<String, String> fields = new LinkedHashMap<>();

                    for(int i = 3; i < command.size(); i += 2){
                        fields.put(command.get(i), command.get(i + 1));
                    }

                    stream.entries().add(new StreamEntry(id, fields));

                    byte[] arg = id.getBytes(StandardCharsets.UTF_8);
                    out.write(("$" + arg.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                    out.write(arg);
                    out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                }

                else if(name.equalsIgnoreCase("TYPE")){
                    String key = command.get(1);
                    Entry entry = store.get(key);

                    if(entry == null){
                        out.write("+none\r\n".getBytes(StandardCharsets.UTF_8));
                    }
                    else if(entry.value() instanceof String){
                        out.write(("+string\r\n").getBytes(StandardCharsets.UTF_8));
                    }
                    else if(entry.value() instanceof RedisStream){
                        out.write(("+stream\r\n").getBytes(StandardCharsets.UTF_8));
                    }
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