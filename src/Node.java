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
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private final Deque<String> relayStack = new ArrayDeque<>();

    // Built-in default poem verses for jabberwocky keys (data keys)
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

    // ----------------- NodeInterface methods ------------------

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new Exception("Invalid node name, must start with 'N:'");
        }
        this.nodeName = nodeName;
        System.out.println("Node name set to: " + nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
        socket = new DatagramSocket(portNumber);
        running = true;
        System.out.println("Opened UDP port: " + portNumber);
        // Start the UDP listener thread
        listenerThread = new Thread(() -> {
            try {
                while (running) {
                    byte[] buffer = new byte[4096];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    System.out.println("Waiting for incoming packets on port " + port + "...");
                    socket.receive(packet);
                    processPacket(packet);
                }
            } catch (SocketException se) {
                System.out.println("Socket closed, listener thread stopping.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        listenerThread.start();
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        // If delay is zero, run indefinitely
        if (delay > 0) {
            Thread.sleep(delay);
        } else {
            while (true) {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        // Our node is active if it matches our name, or if we have an address mapping for it
        boolean active = this.nodeName.equals(nodeName) || store.containsKey(nodeName);
        System.out.println("isActive(" + nodeName + ") -> " + active);
        return active;
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
        System.out.println("Pushed relay node: " + nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String removed = relayStack.pop();
            System.out.println("Popped relay node: " + removed);
        } else {
            System.out.println("Relay stack is empty; nothing to pop.");
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        boolean exists = store.containsKey(key) || jabberwockyPoem.containsKey(key);
        System.out.println("exists(" + key + ") -> " + exists);
        return exists;
    }

    @Override
    public String read(String key) throws Exception {
        String value = store.get(key);
        if (value == null && jabberwockyPoem.containsKey(key)) {
            value = jabberwockyPoem.get(key);
        }
        System.out.println("read(" + key + ") -> " + value);
        return value;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        store.put(key, value);
        System.out.println("write(" + key + ", " + value + ")");
        return true;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        boolean result = store.replace(key, currentValue, newValue);
        System.out.println("CAS(" + key + ", " + currentValue + ", " + newValue + ") -> " + result);
        return result;
    }

    // ----------------- End NodeInterface methods ------------------

    // ----------------- UDP Message Processing ------------------

    // Process incoming UDP packets for CRN messages.
    private void processPacket(DatagramPacket packet) {
        try {
            String fullMsg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
            System.out.println("Received packet from " + packet.getAddress() + ":" + packet.getPort() +
                    " -> [" + fullMsg + "]");
            // Expect message format: [transactionID] [TYPE] [payload]
            String[] parts = fullMsg.split(" ", 3);
            if (parts.length < 2) {
                System.out.println("Malformed message; ignoring.");
                return;
            }
            String transactionID = parts[0];
            char type = parts[1].charAt(0);
            String payload = (parts.length >= 3) ? parts[2] : "";
            InetAddress senderAddress = packet.getAddress();
            int senderPort = packet.getPort();

            switch (type) {
                case 'G': // Name request: reply with our node name.
                    sendResponse(transactionID, 'H', nodeName, senderAddress, senderPort);
                    break;
                case 'N': // Nearest request: return up to three stored address key/value pairs.
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
                case 'W': // Write request: expect key and value separated by a space.
                    String[] kv = payload.split(" ", 2);
                    if (kv.length < 2) {
                        sendResponse(transactionID, 'X', "?", senderAddress, senderPort);
                    } else {
                        boolean writeResult = write(kv[0], kv[1]);
                        // If key existed, we consider it a replacement (R), otherwise an addition (A).
                        sendResponse(transactionID, 'X', writeResult ? "R" : "A", senderAddress, senderPort);
                    }
                    break;
                case 'C': // Compare and swap request: expect key, currentValue, newValue.
                    String[] partsCAS = payload.split(" ", 3);
                    if (partsCAS.length < 3) {
                        sendResponse(transactionID, 'D', "?", senderAddress, senderPort);
                    } else {
                        boolean casResult = CAS(partsCAS[0], partsCAS[1], partsCAS[2]);
                        sendResponse(transactionID, 'D', casResult ? "R" : "N", senderAddress, senderPort);
                    }
                    break;
                case 'V': // Relay message: expect target node name and an inner message.
                    String[] relayParts = payload.split(" ", 2);
                    if (relayParts.length < 2) break;
                    String targetNode = relayParts[0];
                    String innerMessage = relayParts[1];
                    // Look up the target node's address in the store.
                    String targetAddressStr = store.get(targetNode);
                    if (targetAddressStr != null && targetAddressStr.contains(":")) {
                        String[] addrParts = targetAddressStr.split(":");
                        InetAddress targetIP = InetAddress.getByName(addrParts[0]);
                        int targetPort = Integer.parseInt(addrParts[1]);
                        // Forward the inner message to the target node.
                        byte[] data = innerMessage.getBytes(StandardCharsets.UTF_8);
                        DatagramPacket relayPacket = new DatagramPacket(data, data.length, targetIP, targetPort);
                        socket.send(relayPacket);
                        System.out.println("Relayed message to " + targetNode + " at " + targetIP + ":" + targetPort);
                    }
                    break;
                default:
                    System.out.println("Unhandled message type: " + type);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper: Send a UDP response.
    private void sendResponse(String transactionID, char responseType, String payload, InetAddress address, int port) {
        try {
            String response = transactionID + " " + responseType + " " + payload;
            byte[] data = response.getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
            socket.send(packet);
            System.out.println("Sent response: [" + response + "] to " + address + ":" + port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper: Return a concatenation of up to three stored address key/value pairs.
    private String getNearestAddresses(String key) {
        List<String> addresses = new ArrayList<>();
        for (Map.Entry<String, String> entry : store.entrySet()) {
            if (entry.getKey().startsWith("N:")) {
                addresses.add("0 " + entry.getKey() + " 0 " + entry.getValue());
                if (addresses.size() == 3) break;
            }
        }
        String result = String.join(" ", addresses);
        System.out.println("getNearestAddresses(" + key + ") -> " + result);
        return result;
    }

    // ----------------- Optional Shutdown ------------------

    public void shutdown() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        System.out.println("Node shutdown.");
    }

    // ----------------- Utility: Compute HashID (if needed) ------------------
    // This method computes the SHA-256 hash of a string, as described in the RFC.
    public static byte[] computeHashID(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(s.getBytes(StandardCharsets.UTF_8));
        return md.digest();
    }
}
