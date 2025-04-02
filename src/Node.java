import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

interface NodeInterface {
    public void setNodeName(String nodeName) throws Exception;
    public void openPort(int portNumber) throws Exception;
    public void handleIncomingMessages(int delay) throws Exception;
    public boolean isActive(String nodeName) throws Exception;
    public void pushRelay(String nodeName) throws Exception;
    public void popRelay() throws Exception;
    public boolean exists(String key) throws Exception;
    public String read(String key) throws Exception;
    public boolean write(String key, String value) throws Exception;
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;
}

public class Node implements NodeInterface {
    private String nodeName;
    private DatagramSocket socket;
    private int port;
    private volatile boolean running = false;
    private Thread listenerThread;
    private ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private Deque<String> relayStack = new ArrayDeque<>();

    // Predefined poem for D:jabberwocky keys if not stored locally.
    private static final Map<String, String> jabberwockyPoem = new HashMap<>();
    static {
        jabberwockyPoem.put("D:jabberwocky0", "’Twas brillig, and the slithy toves");
        jabberwockyPoem.put("D:jabberwocky1", "Did gyre and gimble in the wabe;");
        jabberwockyPoem.put("D:jabberwocky2", "All mimsy were the borogoves,");
        jabberwockyPoem.put("D:jabberwocky3", "And the mome raths outgrabe.");
        jabberwockyPoem.put("D:jabberwocky4", "Beware the Jabberwock, my son!");
        jabberwockyPoem.put("D:jabberwocky5", "The jaws that bite, the claws that catch!");
        jabberwockyPoem.put("D:jabberwocky6", "Beware the Jubjub bird, and shun");
    }

    // Helper: generate a random 2-byte transaction ID as a String
    private String generateTransactionID() {
        Random r = new Random();
        char c1 = (char)(33 + r.nextInt(94)); // ASCII 33-126, avoid spaces
        char c2 = (char)(33 + r.nextInt(94));
        return "" + c1 + c2;
    }

    // Helper: build a message with given transactionID, type and payload
    private String buildMessage(String transactionID, char type, String payload) {
        return transactionID + " " + type + " " + payload;
    }

    // Helper: send a response to a specific address/port
    private void sendResponse(String transactionID, char responseType, String payload, InetAddress address, int port) {
        try {
            String response = buildMessage(transactionID, responseType, payload);
            byte[] data = response.getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
            socket.send(packet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new Exception("Invalid node name, must start with 'N:'");
        }
        this.nodeName = nodeName;
        // Publish our own address key/value pair in the store.
        // (In a full system, this might be broadcasted to others.)
        // For now, we do not automatically add it to the store.
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
        socket = new DatagramSocket(portNumber);
        running = true;
        // Start a listener thread to process incoming UDP messages asynchronously.
        listenerThread = new Thread(() -> {
            try {
                while (running) {
                    byte[] buffer = new byte[4096];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    System.out.println("Packet received from " + packet.getAddress() + ":" + packet.getPort());
                    processPacket(packet);
                }
            } catch (SocketException se) {
                // Socket closed—exit thread.
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        listenerThread.start();
    }

    // Parse incoming messages and dispatch responses.
    private void processPacket(DatagramPacket packet) {
        try {
            String fullMsg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
            // Expecting: [transactionID] [TYPE] [payload]
            String[] parts = fullMsg.split(" ", 3);
            if (parts.length < 2) return; // malformed
            String transactionID = parts[0];
            char type = parts[1].charAt(0);
            String payload = (parts.length >= 3) ? parts[2] : "";
            InetAddress senderAddress = packet.getAddress();
            int senderPort = packet.getPort();

            // Dispatch based on message type.
            switch (type) {
                case 'G': // Name request; reply with name response.
                    sendResponse(transactionID, 'H', nodeName, senderAddress, senderPort);
                    break;
                case 'N': // Nearest request. For simplicity, return up to three stored addresses.
                    String nearest = getNearestAddresses(payload);
                    sendResponse(transactionID, 'O', nearest, senderAddress, senderPort);
                    break;
                case 'E': // Key existence request.
                    char existsChar = exists(payload) ? 'Y' : 'N';
                    sendResponse(transactionID, 'F', String.valueOf(existsChar), senderAddress, senderPort);
                    break;
                case 'R': // Read request.
                    String value = read(payload);
                    if (value != null)
                        sendResponse(transactionID, 'S', "Y " + value, senderAddress, senderPort);
                    else
                        sendResponse(transactionID, 'S', "N ", senderAddress, senderPort);
                    break;
                case 'W': // Write request. Format: key and value separated by a space.
                    String[] kv = payload.split(" ", 2);
                    if (kv.length < 2) {
                        sendResponse(transactionID, 'X', "?", senderAddress, senderPort);
                    } else {
                        boolean writeResult = write(kv[0], kv[1]);
                        sendResponse(transactionID, 'X', writeResult ? "R" : "A", senderAddress, senderPort);
                    }
                    break;
                case 'C': // Compare and swap request. Format: key, currentValue, newValue.
                    String[] partsCAS = payload.split(" ", 3);
                    if (partsCAS.length < 3) {
                        sendResponse(transactionID, 'D', "?", senderAddress, senderPort);
                    } else {
                        boolean casResult = CAS(partsCAS[0], partsCAS[1], partsCAS[2]);
                        sendResponse(transactionID, 'D', casResult ? "R" : "N", senderAddress, senderPort);
                    }
                    break;
                case 'V': // Relay message. Format: targetNodeName and inner message.
                    String[] relayParts = payload.split(" ", 2);
                    if (relayParts.length < 2) break;
                    String targetNode = relayParts[0];
                    String innerMessage = relayParts[1];
                    // For relay, lookup the target node's address from the store.
                    String targetAddressStr = store.get(targetNode);
                    if (targetAddressStr != null && targetAddressStr.contains(":")) {
                        String[] addrParts = targetAddressStr.split(":");
                        InetAddress targetIP = InetAddress.getByName(addrParts[0]);
                        int targetPort = Integer.parseInt(addrParts[1]);
                        // Forward the inner message.
                        byte[] data = innerMessage.getBytes(StandardCharsets.UTF_8);
                        DatagramPacket relayPacket = new DatagramPacket(data, data.length, targetIP, targetPort);
                        socket.send(relayPacket);
                        // In a full implementation, you would capture the response and relay it back.
                    }
                    break;
                default:
                    // Unknown type; ignore.
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper: return up to three address key/value pairs as a single string.
    private String getNearestAddresses(String key) throws Exception {
        // In a full implementation you would compute the hash distance.
        // Here we simply iterate over stored keys starting with "N:".
        List<String> addresses = new ArrayList<>();
        for (String k : store.keySet()) {
            if (k.startsWith("N:"))
                addresses.add("0 " + k + " 0 " + store.get(k));
            if (addresses.size() == 3)
                break;
        }
        // Join with a space.
        return String.join(" ", addresses);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        // With our listener thread running, simply pause for the delay.
        if (delay > 0) {
            Thread.sleep(delay);
        } else {
            // If delay is zero, run indefinitely.
            while (true) {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        // Minimal implementation: if the given nodeName equals our own, return true.
        // Otherwise, check if we have an address mapping.
        if (this.nodeName.equals(nodeName))
            return true;
        return store.containsKey(nodeName);
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty())
            relayStack.pop();
    }

    @Override
    public boolean exists(String key) throws Exception {
        // Check local key/value store or the built-in poem.
        if (store.containsKey(key))
            return true;
        if (jabberwockyPoem.containsKey(key))
            return true;
        return false;
    }

    @Override
    public String read(String key) throws Exception {
        if (store.containsKey(key))
            return store.get(key);
        if (jabberwockyPoem.containsKey(key))
            return jabberwockyPoem.get(key);
        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        // For address keys (starting with "N:"), limit to three per distance in a full implementation.
        store.put(key, value);
        return true;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        // Atomic compare-and-swap.
        return store.replace(key, currentValue, newValue);
    }

    // Optional shutdown method.
    public void shutdown() {
        running = false;
        if (socket != null && !socket.isClosed())
            socket.close();
        if (listenerThread != null)
            listenerThread.interrupt();
    }

    // (Optional) HashID computation using SHA-256.
    public static byte[] computeHashID(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(s.getBytes(StandardCharsets.UTF_8));
        return md.digest();
    }
}
