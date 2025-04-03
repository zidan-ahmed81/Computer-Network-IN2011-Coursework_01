import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    // Node identity and communication
    private String nodeName;
    private DatagramSocket socket;

    // Local key/value stores
    private Map<String, String> localStore = new ConcurrentHashMap<>();
    private Map<String, String> dataStore = new ConcurrentHashMap<>();

    // Routing table and relay stack
    private List<InetSocketAddress> knownNodes = Collections.synchronizedList(new ArrayList<>());
    private List<InetSocketAddress> neighbors = Collections.synchronizedList(new ArrayList<>());
    private ArrayDeque<String> relayStack = new ArrayDeque<>();

    // Constructor (all collections are initialized above)
    public Node() {
        // Further initialization (if required) can go here.
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new Exception("Node name must start with 'N:'");
        }
        this.nodeName = nodeName;
        System.out.println("Node name set to: " + nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
        socket.setSoTimeout(1000); // Timeout to periodically check the delay in handleIncomingMessages()
        System.out.println("Opened port: " + portNumber);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        System.out.println("Listening for incoming messages...");
        long endTime = System.currentTimeMillis() + delay;
        while (System.currentTimeMillis() < endTime) {
            try {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                processPacket(packet);
            } catch (SocketTimeoutException e) {
                // Continue waiting until the overall delay is reached.
            }
        }
        System.out.println("Timeout reached, exiting handleIncomingMessages()");
    }

    // Process incoming packets according to the CRN protocol
    private void processPacket(DatagramPacket packet) throws Exception {
        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        System.out.println("Received packet from " + packet.getAddress() + ":" + packet.getPort() + " -> " + message);

        // Basic message parsing: messages are expected to start with a two-character transaction ID,
        // followed by a message type (e.g., "G" for a name request or "R" for a read request)
        String[] parts = message.split(" ", 4);
        if (parts.length < 2) return;
        String txID = parts[0];
        String type = parts[1];

        switch (type) {
            case "G": // Name Request: respond with our node name.
                // Expected response format: "<txID> H <nodeName>"
                String nameResponse = txID + " H " + nodeName;
                byte[] respBytes = nameResponse.getBytes(StandardCharsets.UTF_8);
                DatagramPacket respPacket = new DatagramPacket(respBytes, respBytes.length, packet.getAddress(), packet.getPort());
                socket.send(respPacket);
                System.out.println("Sent name response: " + nameResponse);
                break;
            case "R": // Read Request: "<txID> R <key>"
                if (parts.length < 3) return;
                String key = parts[2];
                if (localStore.containsKey(key)) {
                    // Condition A true: send 'Y' with the stored value.
                    String value = localStore.get(key);
                    String readResponse = txID + " S Y " + value;
                    byte[] readRespBytes = readResponse.getBytes(StandardCharsets.UTF_8);
                    DatagramPacket readRespPacket = new DatagramPacket(readRespBytes, readRespBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(readRespPacket);
                    System.out.println("Sent read response (Y) for key: " + key);
                } else {
                    // Key not found locally. For simplicity, send a 'N' response.
                    String readResponse = txID + " S N ";
                    byte[] readRespBytes = readResponse.getBytes(StandardCharsets.UTF_8);
                    DatagramPacket readRespPacket = new DatagramPacket(readRespBytes, readRespBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(readRespPacket);
                    System.out.println("Sent read response (N) for key: " + key);
                }
                break;
            // Future implementations for Write ("W"), CAS ("C"), and Relay ("V") messages would be added here.
            default:
                System.out.println("Unknown message type: " + type);
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        // For this basic implementation, simply return true.
        // In a full implementation, you might send a name request and wait for a valid response.
        return true;
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        // Check both the localStore and dataStore.
        return localStore.containsKey(key) || dataStore.containsKey(key);
    }

    // Read method: checks the local store, then broadcasts a read request to all bootstrapped neighbors.
    @Override
    public String read(String key) throws Exception {
        if (localStore.containsKey(key)) return localStore.get(key);

        String txID = generateTxID();
        String requestMessage = txID + " R " + key;
        byte[] requestBytes = requestMessage.getBytes(StandardCharsets.UTF_8);

        // Send the read request to all neighbors (assumed to be bootstrapped)
        for (InetSocketAddress neighbor : neighbors) {
            DatagramPacket packet = new DatagramPacket(requestBytes, requestBytes.length,
                    neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);
        }

        // Wait for a response for up to 5 seconds.
        byte[] buffer = new byte[1024];
        DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 5000) {
            try {
                socket.setSoTimeout(5000);
                socket.receive(responsePacket);
                String response = new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
                if (response.startsWith(txID)) {
                    String[] parts = response.split(" ", 4);
                    if (parts.length >= 3 && "Y".equals(parts[2]) && parts.length == 4) {
                        return parts[3];
                    }
                }
            } catch (SocketTimeoutException e) {
                break;
            }
        }
        return null;
    }

    // Write method: store the key/value pair locally and in the dataStore.
    @Override
    public boolean write(String key, String value) throws Exception {
        localStore.put(key, value);
        dataStore.put(key, value);
        // In a full CRN implementation, you'd also propagate the write to the nearest nodes.
        return true;
    }

    // Compare-And-Swap (CAS) method: updates the key if the current value matches.
    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (localStore.containsKey(key) && localStore.get(key).equals(currentValue)) {
            localStore.put(key, newValue);
            dataStore.put(key, newValue);
            return true;
        }
        return false;
    }

    // Bootstrap method: initializes the neighbors list using the Azure Lab nodes.
    // For demonstration, it uses two IP addresses and a range of ports.
    public void bootstrap() throws Exception {
        neighbors.clear();  // Clear any existing neighbors.
        String[] bootstrapIPs = {"10.200.51.18", "10.200.51.19"};
        for (String ip : bootstrapIPs) {
            InetAddress addr = InetAddress.getByName(ip);
            for (int port = 20110; port <= 20116; port++) {
                InetSocketAddress neighborAddr = new InetSocketAddress(addr, port);
                // Optionally skip adding your own address (if applicable)
                if (socket != null && socket.getLocalPort() == port) continue;
                neighbors.add(neighborAddr);
            }
        }
        System.out.println("Bootstrapped neighbors:");
        for (InetSocketAddress n : neighbors) {
            System.out.println("  " + n.getAddress().getHostAddress() + ":" + n.getPort());
        }
    }

    // Helper method to generate a two-character transaction ID.
    private String generateTxID() {
        Random random = new Random();
        char c1 = (char) ('A' + random.nextInt(26));
        char c2 = (char) ('A' + random.nextInt(26));
        return "" + c1 + c2;
    }

    // Additional method for graceful shutdown.
    public void shutdown() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
