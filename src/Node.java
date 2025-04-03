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

    private String nodeName;

    private DatagramSocket socket;

    private Map<String, String> localStore = new ConcurrentHashMap<>();

    private List<InetSocketAddress> knownNodes = Collections.synchronizedList(new ArrayList<>());
    private ArrayDeque<String> relayStack;
    private HashMap<Object, Object> dataStore;
    private List<InetSocketAddress> neighbors = new ArrayList<>();



    @Override
    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        System.out.println("Node name set to: " + nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
        socket.setSoTimeout(1000); // Check every second when using a delay
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
    private void processPacket(DatagramPacket packet) {
        String message = new String(packet.getData(), 0, packet.getLength());
        System.out.println("Received packet from "
                + packet.getAddress() + ":" + packet.getPort()
                + " -> " + message);
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        // Minimal implementation always returns true.
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
        return dataStore.containsKey(key);
    }

    // Bootstrap method that initializes the routing table.
// For demonstration we add the Azure lab nodes (10.200.51.18 and 10.200.51.19 on ports 20110 to 20116).
    public void bootstrap() throws Exception {
        neighbors.clear();  // Ensure the list is empty before bootstrapping.
        String[] bootstrapIPs = {"10.200.51.18", "10.200.51.19"};
        for (String ip : bootstrapIPs) {
            InetAddress addr = InetAddress.getByName(ip);
            for (int port = 20110; port <= 20116; port++) {
                // Optional: avoid adding your own address.
                // For example, if your node's port is 20110 and your name indicates your identity,
                // you could check and skip that entry.
                InetSocketAddress neighborAddress = new InetSocketAddress(addr, port);
                // For now, add all provided bootstrap addresses.
                neighbors.add(neighborAddress);
            }
        }
        System.out.println("Bootstrapped neighbors:");
        for (InetSocketAddress n : neighbors) {
            System.out.println("  " + n.getAddress().getHostAddress() + ":" + n.getPort());
        }
    }

    // Helper method to generate a transaction ID.
    private String generateTxID() {
        Random random = new Random();
        char c1 = (char) ('A' + random.nextInt(26));
        char c2 = (char) ('A' + random.nextInt(26));
        return "" + c1 + c2;
    }

    // The read method (simplified example).
    @Override
    public String read(String key) throws Exception {
        // Check local store first.
        if (localStore.containsKey(key)) return localStore.get(key);

        String txID = generateTxID();
        String requestMessage = txID + " R " + key;
        byte[] requestBytes = requestMessage.getBytes(StandardCharsets.UTF_8);

        // Use the neighbors list from bootstrapping.
        for (InetSocketAddress neighbor : neighbors) {
            DatagramPacket packet = new DatagramPacket(requestBytes, requestBytes.length,
                    neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);
        }

        // Wait for a response (simplified with a timeout).
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
                    if (parts.length >= 3) {
                        String responseChar = parts[2];
                        if ("Y".equals(responseChar) && parts.length == 4) {
                            return parts[3];
                        }
                    }
                }
            } catch (SocketTimeoutException e) {
                break;
            }
        }
        return null;
    }


    @Override
    public boolean write(String key, String value) throws Exception {
        // For this minimal version, simply put the key/value pair in our store.
        dataStore.put(key, value);
        return true;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        // Use the atomic replace method from ConcurrentHashMap.
        return dataStore.replace(key, currentValue, newValue);
    }

    // Additional method for graceful shutdown
    public void shutdown() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
