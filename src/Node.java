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
    private Map<String, String> localStore;
    private Map<String, String> dataStore;

    // Routing table and relay stack
    private List<InetSocketAddress> knownNodes;
    private List<InetSocketAddress> neighbors;
    private ArrayDeque<String> relayStack;

    // Constructor: initialize all collections
    public Node() {
        localStore = new ConcurrentHashMap<>();
        dataStore = new ConcurrentHashMap<>();
        knownNodes = Collections.synchronizedList(new ArrayList<>());
        neighbors = new ArrayList<>();
        relayStack = new ArrayDeque<>();
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
        socket.setSoTimeout(1000); // Timeout to periodically check in handleIncomingMessages()
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
                // Continue waiting until endTime is reached.
            }
        }
        System.out.println("Timeout reached, exiting handleIncomingMessages()");
    }

    // Helper method to process incoming packets
    private void processPacket(DatagramPacket packet) throws Exception {
        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        System.out.println("Received packet from " + packet.getAddress() + ":" + packet.getPort() + " -> " + message);

        // Basic parsing: expect message format with a transaction ID and type.
        String[] parts = message.split(" ", 4);
        if (parts.length < 2) return;
        String txID = parts[0];
        String type = parts[1];

        switch (type) {
            case "G": // Name Request
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
                    String value = localStore.get(key);
                    String readResponse = txID + " S Y " + value;
                    byte[] readRespBytes = readResponse.getBytes(StandardCharsets.UTF_8);
                    DatagramPacket readRespPacket = new DatagramPacket(readRespBytes, readRespBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(readRespPacket);
                    System.out.println("Sent read response (Y) for key: " + key);
                } else {
                    String readResponse = txID + " S N ";
                    byte[] readRespBytes = readResponse.getBytes(StandardCharsets.UTF_8);
                    DatagramPacket readRespPacket = new DatagramPacket(readRespBytes, readRespBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(readRespPacket);
                    System.out.println("Sent read response (N) for key: " + key);
                }
                break;
            default:
                System.out.println("Unknown message type: " + type);
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        return true;  // Minimal implementation always returns true.
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
        return localStore.containsKey(key) || dataStore.containsKey(key);
    }

    // Read: check local store then broadcast a read request to neighbors.
    @Override
    public String read(String key) throws Exception {
        if (localStore.containsKey(key)) return localStore.get(key);

        String txID = generateTxID();
        String requestMessage = txID + " R " + key;
        byte[] requestBytes = requestMessage.getBytes(StandardCharsets.UTF_8);

        // Iterate over the neighbors list (ensured not null by constructor)
        for (InetSocketAddress neighbor : neighbors) {
            DatagramPacket packet = new DatagramPacket(requestBytes, requestBytes.length,
                    neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);
        }

        // Wait for a response up to 5 seconds.
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

    // Write: store the key/value pair locally.
    @Override
    public boolean write(String key, String value) throws Exception {
        localStore.put(key, value);
        dataStore.put(key, value);
        return true;
    }

    // CAS: update the key if the current value matches.
    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (localStore.containsKey(key) && localStore.get(key).equals(currentValue)) {
            localStore.put(key, newValue);
            dataStore.put(key, newValue);
            return true;
        }
        return false;
    }

    // Bootstrap: initialize the neighbors list with the Azure Lab nodes.
    public void bootstrap() throws Exception {
        neighbors.clear();
        String[] bootstrapIPs = {"10.200.51.18", "10.200.51.19"};
        for (String ip : bootstrapIPs) {
            InetAddress addr = InetAddress.getByName(ip);
            for (int port = 20110; port <= 20116; port++) {
                InetSocketAddress neighborAddr = new InetSocketAddress(addr, port);
                if (socket != null && socket.getLocalPort() == port) continue;
                neighbors.add(neighborAddr);
            }
        }
        System.out.println("Bootstrapped neighbors:");
        for (InetSocketAddress n : neighbors) {
            System.out.println("  " + n.getAddress().getHostAddress() + ":" + n.getPort());
        }
    }

    // Helper method: generate a two-character transaction ID.
    private String generateTxID() {
        Random random = new Random();
        char c1 = (char) ('A' + random.nextInt(26));
        char c2 = (char) ('A' + random.nextInt(26));
        return "" + c1 + c2;
    }

    // Graceful shutdown: close the UDP socket.
    public void shutdown() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
