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
import java.util.HashMap;

public class Main {
    private record Entry(Object value, long expiresAt) {}
    private record StreamEntry(String id, Map<String, String> fields){}
    private record RedisStream(List<StreamEntry> entries){}

    private static final Map<String, Entry> store = new ConcurrentHashMap<>();
    private static final Map<String, Queue<BlockingQueue<String>>> waiters = new ConcurrentHashMap<>();
    private static final Map<String, Queue<BlockingQueue<String>>> streamWaiters = new ConcurrentHashMap<>();
    private static final Map<String, Long> versions = new ConcurrentHashMap<>();

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
            Map<String, Long> watched = new HashMap<>();
            List<String> command;
            boolean inMulti = false;
            List<List<String>> queued = new ArrayList<>();

            while ((command = readCommand(in)) != null) {
                String name = command.get(0);

                if(name.equalsIgnoreCase("MULTI")){
                    inMulti = true;
                    out.write("+OK\r\n".getBytes(StandardCharsets.UTF_8));
                    continue;
                }

                if(name.equalsIgnoreCase("EXEC")){
                    if(!inMulti){
                        out.write("-ERR EXEC without MULTI\r\n".getBytes(StandardCharsets.UTF_8));
                        continue;
                    }

                    boolean dirty = false;

                    for(Map.Entry<String, Long> w : watched.entrySet()){
                        long now = versions.getOrDefault(w.getKey(), 0L);
                        if(now != w.getValue()){
                            dirty = true;
                            break;
                        }
                    }

                    if(dirty){
                        out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
                    }
                    else{
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                        for(List<String> queuedCommand : queued){
                            execute(queuedCommand, buffer);
                        }

                        out.write(("*" + queued.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.write(buffer.toByteArray());
                    }

                    queued.clear();
                    inMulti = false;
                    watched.clear();
                    continue;
                }

                if(name.equalsIgnoreCase("DISCARD")){
                    if(!inMulti){
                        out.write("-ERR DISCARD without MULTI\r\n".getBytes(StandardCharsets.UTF_8));
                        continue;
                    }
                    queued.clear();
                    inMulti = false;
                    out.write("+OK\r\n".getBytes(StandardCharsets.UTF_8));
                    continue;
                }

                if(name.equalsIgnoreCase("WATCH")){
                    if(inMulti){
                        out.write("-ERR WATCH inside MULTI is not allowed\r\n".getBytes(StandardCharsets.UTF_8));
                        continue;
                    }
                    String key = command.get(1);

                    watched.put(key, versions.getOrDefault(key, 0L));
                    out.write("+OK\r\n".getBytes(StandardCharsets.UTF_8));
                    continue;
                }

                if(inMulti){
                    queued.add(command);
                    out.write("+QUEUED\r\n".getBytes(StandardCharsets.UTF_8));
                    continue;
                }

                execute(command, out);
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void execute(List<String> command, OutputStream out) throws IOException {
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
            versions.merge(key, 1L, Long::sum);
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
            int size;

            synchronized (waiters) {
                Entry entry = store.get(key);
                List<String> list;

                if (entry == null) {
                    list = new ArrayList<>();
                    store.put(key, new Entry(list, Long.MAX_VALUE));
                } else {
                    list = (List<String>) entry.value();
                }
                list.addAll(command.subList(2, command.size()));
                size = list.size();

                Queue<BlockingQueue<String>> line = waiters.get(key);
                if (line != null) {
                    BlockingQueue<String> tray = line.poll();
                    if (tray != null) {
                        String element = list.remove(0);
                        tray.offer(element);
                    }
                }
            }

            out.write((":" + size + "\r\n").getBytes(StandardCharsets.UTF_8));
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
        else if (name.equalsIgnoreCase("BLPOP")) {
            String key = command.get(1);
            double seconds = Double.parseDouble(command.get(2));
            long timeoutMs = (long) (seconds * 1000);

            String element = null;
            BlockingQueue<String> tray = null;
            Queue<BlockingQueue<String>> line = null;

            synchronized (waiters) {
                Entry entry = store.get(key);
                List<String> list = null;

                if (entry != null) {
                    list = (List<String>) entry.value();
                }

                if (list != null && list.size() > 0) {
                    element = list.remove(0);
                } else {
                    line = waiters.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>());
                    tray = new LinkedBlockingQueue<>();
                    line.add(tray);
                }
            }

            if (element == null) {
                try {
                    if (timeoutMs == 0) {
                        element = tray.take();
                    } else {
                        element = tray.poll(timeoutMs, TimeUnit.MILLISECONDS);
                        if (element == null && !line.remove(tray)) {
                            element = tray.take();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            if (element == null) {
                out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
            } else {
                out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));

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
                return;
            }

            if(newMs < lastMs || (newMs == lastMs && newSeq <= lastSeq)){
                out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes(StandardCharsets.UTF_8));
                return;
            }

            Map<String, String> fields = new LinkedHashMap<>();

            for(int i = 3; i < command.size(); i += 2){
                fields.put(command.get(i), command.get(i + 1));
            }

            stream.entries().add(new StreamEntry(id, fields));

            Queue<BlockingQueue<String>> line = streamWaiters.get(key);
            if(line != null){
                BlockingQueue<String> tray;
                while((tray = line.poll()) != null){
                    tray.offer(key);
                }
            }

            byte[] arg = id.getBytes(StandardCharsets.UTF_8);
            out.write(("$" + arg.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.write(arg);
            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        else if(name.equalsIgnoreCase("XRANGE")){
            String key = command.get(1);
            String startStr = command.get(2);
            String endStr = command.get(3);
            Entry entry = store.get(key);

            if(entry == null){
                out.write("*0\r\n".getBytes(StandardCharsets.UTF_8));
                return;
            }

            RedisStream stream = (RedisStream) entry.value();
            long startMs;
            long startSeq;

            if(startStr.equals("-")){
                startMs = 0;
                startSeq = 0;
            }
            else{
                String[] sp_start = startStr.split("-");
                startMs = Long.parseLong(sp_start[0]);
                startSeq = 0;

                if(sp_start.length > 1){
                    startSeq = Long.parseLong(sp_start[1]);
                }
            }

            long endMs;
            long endSeq;

            if(endStr.equals("+")){
                endMs = Long.MAX_VALUE;
                endSeq = Long.MAX_VALUE;
            }
            else{
                String[] sp_end = endStr.split("-");
                endMs = Long.parseLong(sp_end[0]);
                endSeq = Long.MAX_VALUE;

                if(sp_end.length > 1){
                    endSeq = Long.parseLong(sp_end[1]);
                }
            }

            List<StreamEntry> result = new ArrayList<>();

            for(StreamEntry e : stream.entries){
                String[] sp = e.id.split("-");
                long ms = Long.parseLong(sp[0]);
                long seq = Long.parseLong(sp[1]);

                boolean afterStart = ms > startMs || (ms == startMs && seq >= startSeq);
                boolean afterEnd = ms < endMs || (ms == endMs && seq <= endSeq);

                if(afterStart && afterEnd){
                    result.add(e);
                }
            }

            out.write(("*" + result.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

            for(StreamEntry e : result){
                out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));

                byte[] idBytes = e.id().getBytes(StandardCharsets.UTF_8);
                out.write(("$" + idBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                out.write(idBytes);
                out.write("\r\n".getBytes(StandardCharsets.UTF_8));

                out.write(("*" + (e.fields().size() * 2) + "\r\n").getBytes(StandardCharsets.UTF_8));

                for(Map.Entry<String, String> f : e.fields().entrySet()){
                    byte[] fieldKey = f.getKey().getBytes(StandardCharsets.UTF_8);
                    out.write(("$" + fieldKey.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                    out.write(fieldKey);
                    out.write("\r\n".getBytes(StandardCharsets.UTF_8));

                    byte[] fieldValue = f.getValue().getBytes(StandardCharsets.UTF_8);
                    out.write(("$" + fieldValue.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                    out.write(fieldValue);
                    out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        else if(name.equalsIgnoreCase("XREAD")){
            int streamsIdx = -1;

            for(int i = 0; i < command.size(); i++){
                if(command.get(i).equalsIgnoreCase("streams")){
                    streamsIdx = i;
                    break;
                }
            }

            int n = (command.size() - (streamsIdx + 1)) / 2;

            long blockMs = -1;
            if(command.get(1).equalsIgnoreCase("block")){
                blockMs = Long.parseLong(command.get(2));
            }

            for(int i = 0; i < n; i++){
                int idIdx = streamsIdx + 1 + n + i;

                if(command.get(idIdx).equalsIgnoreCase("$")){
                    String key = command.get(streamsIdx + 1 + i);
                    Entry entry = store.get(key);
                    String lastId = "0-0";

                    if(entry != null){
                        RedisStream stream = (RedisStream) entry.value();

                        if(!stream.entries().isEmpty()){
                            lastId = stream.entries().get(stream.entries().size() - 1).id();
                        }
                    }

                    command.set(idIdx, lastId);
                }
            }

            Map<String, List<StreamEntry>> found = collect(command, streamsIdx, n);

            if(!hasData(found) && blockMs >= 0){
                BlockingQueue<String> tray = new LinkedBlockingQueue<>();

                for(String key : found.keySet()){
                    streamWaiters.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(tray);
                }

                found = collect(command, streamsIdx, n);

                if(!hasData(found)){
                    try{
                        if(blockMs == 0){
                            tray.take();
                        }
                        else{
                            tray.poll(blockMs, TimeUnit.MILLISECONDS);
                        }
                    }
                    catch(InterruptedException ie){
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                for(String key : found.keySet()){
                    Queue<BlockingQueue<String>> line = streamWaiters.get(key);
                    if(line != null){
                        line.remove(tray);
                    }
                }

                found = collect(command, streamsIdx, n);
            }

            if(!hasData(found)){
                out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
                return;
            }

            out.write(("*" + found.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

            for(Map.Entry<String, List<StreamEntry>> s : found.entrySet()){
                out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));

                byte[] keyBytes = s.getKey().getBytes(StandardCharsets.UTF_8);
                out.write(("$" + keyBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                out.write(keyBytes);
                out.write("\r\n".getBytes(StandardCharsets.UTF_8));

                out.write(("*" + s.getValue().size() + "\r\n").getBytes(StandardCharsets.UTF_8));

                for(StreamEntry e : s.getValue()){
                    out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));

                    byte[] idBytes = e.id().getBytes(StandardCharsets.UTF_8);
                    out.write(("$" + idBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                    out.write(idBytes);
                    out.write("\r\n".getBytes(StandardCharsets.UTF_8));

                    out.write(("*" + (e.fields().size() * 2) + "\r\n").getBytes(StandardCharsets.UTF_8));

                    for(Map.Entry<String, String> f : e.fields().entrySet()){
                        byte[] fieldKey = f.getKey().getBytes(StandardCharsets.UTF_8);
                        out.write(("$" + fieldKey.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.write(fieldKey);
                        out.write("\r\n".getBytes(StandardCharsets.UTF_8));

                        byte[] fieldValue = f.getValue().getBytes(StandardCharsets.UTF_8);
                        out.write(("$" + fieldValue.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.write(fieldValue);
                        out.write("\r\n".getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        }
        else if(name.equalsIgnoreCase("INCR")){
            String key = command.get(1);
            Entry entry = store.get(key);

            long n;
            long expiresAt;

            if(entry == null){
                n = 0;
                expiresAt = Long.MAX_VALUE;
            }
            else {
                String value = (String)entry.value();

                try{
                    n = Long.parseLong(value);
                }
                catch(NumberFormatException e){
                    out.write("-ERR value is not an integer or out of range\r\n".getBytes(StandardCharsets.UTF_8));
                    return;
                }
                expiresAt = entry.expiresAt();
            }
            n = n + 1;

            store.put(key, new Entry(String.valueOf(n), expiresAt));
            versions.merge(key, 1L, Long::sum);
            out.write((":" + n + "\r\n").getBytes(StandardCharsets.UTF_8));
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

    private static Map<String, List<StreamEntry>> collect(List<String> command, int streamsIdx, int n){
        Map<String, List<StreamEntry>> found = new LinkedHashMap<>();

        for(int i = 0; i < n; i++){
            String key = command.get(streamsIdx + 1 + i);
            String idStr = command.get(streamsIdx + 1 + n + i);
            Entry entry = store.get(key);

            String[] sp_id = idStr.split("-");
            long idMs = Long.parseLong(sp_id[0]);
            long idSeq = 0;

            if(sp_id.length > 1){
                idSeq = Long.parseLong(sp_id[1]);
            }

            List<StreamEntry> result = new ArrayList<>();

            if(entry != null){
                RedisStream stream = (RedisStream) entry.value();

                for(StreamEntry e : stream.entries()){
                    String[] sp = e.id().split("-");
                    long ms = Long.parseLong(sp[0]);
                    long seq = Long.parseLong(sp[1]);

                    boolean after = ms > idMs || (ms == idMs && seq > idSeq);

                    if(after){
                        result.add(e);
                    }
                }
            }

            found.put(key, result);
        }

        return found;
    }

    private static boolean hasData(Map<String, List<StreamEntry>> found){
        for(List<StreamEntry> result : found.values()){
            if(!result.isEmpty()){
                return true;
            }
        }
        return false;
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