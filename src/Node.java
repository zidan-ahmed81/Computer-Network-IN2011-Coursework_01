// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;


interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {
    // Local key/value store for data keys (keys starting with "D:")
    private ConcurrentMap<String, String> dataStore = new ConcurrentHashMap<>();

    // A stack to maintain relay nodes (if non-empty, all outgoing requests will be relayed)
    private Deque<String> relayStack = new ConcurrentLinkedDeque<>();

    // Store for address key/value pairs (keys starting with "N:")
    private ConcurrentMap<String, String> addressStore = new ConcurrentHashMap<>();

    private String nodeName;
    private byte[] nodeHash;
    DatagramSocket socket;

    // Set the node name and compute its hashID using HashID.computeHashID.
    @Override
    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        this.nodeHash = HashID.computeHashID(nodeName);
        System.out.println("Node name set to " + nodeName);
    }

    // Open a UDP port for sending and receiving messages.
    @Override
    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
        // Store your own address key/value pair using your node name as key.
        String localAddress = InetAddress.getLocalHost().getHostAddress() + ":" + portNumber;
        if (nodeName != null) {
            addressStore.put(nodeName, localAddress);
        }
        System.out.println("Opened UDP port " + portNumber);
    }

    // Listen for incoming UDP messages for a specified delay (in milliseconds).
    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.setSoTimeout(delay > 0 ? delay : 0);
        try {
            while (true) {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                System.out.println("Received: " + message);
                processMessage(message, packet.getAddress(), packet.getPort());
            }
        } catch (SocketTimeoutException ste) {
            // Timeout reached – exit the method.
        }
    }

    // Process incoming messages based on the command.
    private void processMessage(String message, InetAddress senderAddress, int senderPort) {
        String[] parts = message.split(" ", 3);
        if (parts.length < 2) {
            System.out.println("Invalid message format");
            return;
        }
        String transactionId = parts[0];
        String command = parts[1];
        String payload = parts.length >= 3 ? parts[2] : "";
        switch (command) {
            case "G": // Name request – reply with our node name.
                sendNameResponse(transactionId, senderAddress, senderPort);
                break;
            case "R": // Read request – payload is the key.
                sendReadResponse(transactionId, payload, senderAddress, senderPort);
                break;
            case "W": // Write request – payload contains key and value.
                String[] kv = payload.split(" ", 2);
                if (kv.length == 2) {
                    boolean writeResult = false;
                    try {
                        writeResult = write(kv[0], kv[1]);
                    } catch (Exception e) {
                        System.err.println("Error in write: " + e.getMessage());
                    }
                    sendWriteResponse(transactionId, writeResult, senderAddress, senderPort);
                }
                break;
            case "C": // Compare-And-Swap request – payload: key, currentValue, newValue.
                String[] partsCAS = payload.split(" ", 3);
                if (partsCAS.length == 3) {
                    boolean casResult = false;
                    try {
                        casResult = CAS(partsCAS[0], partsCAS[1], partsCAS[2]);
                    } catch (Exception e) {
                        System.err.println("Error in CAS: " + e.getMessage());
                    }
                    sendCASResponse(transactionId, casResult, senderAddress, senderPort);
                }
                break;
            case "N": // Nearest request – payload is a hex-encoded hash.
                processNearestRequest(transactionId, payload, senderAddress, senderPort);
                break;
            case "E": // Key existence request – payload is the key.
                processKeyExistenceRequest(transactionId, payload, senderAddress, senderPort);
                break;
            case "V": // Relay message – payload: target node + inner message.
                processRelayMessage(transactionId, payload, senderAddress, senderPort);
                break;
            case "I": // Information message – log it.
                System.out.println("Received Information message: " + payload);
                break;
            case "S":
                System.out.println("Received an unexpected S response: " + payload);
                break;
            default:
                System.out.println("Unknown command: " + command);
        }
    }

    // Send a name response message.
    private void sendNameResponse(String transactionId, InetAddress address, int port) {
        String response = transactionId + " H " + nodeName;
        sendResponse(response, address, port);
    }

    // Send a read response message based on local data store.
    private void sendReadResponse(String transactionId, String key, InetAddress address, int port) {
        String value = localRead(key);
        String response = (value != null) ? (transactionId + " S Y " + value) : (transactionId + " S N");
        sendResponse(response, address, port);
    }

    // Send a write response message.
    private void sendWriteResponse(String transactionId, boolean success, InetAddress address, int port) {
        String response = transactionId + " X " + (success ? "A" : "X");
        sendResponse(response, address, port);
    }

    // Send a CAS response message.
    private void sendCASResponse(String transactionId, boolean success, InetAddress address, int port) {
        String response = transactionId + " D " + (success ? "R" : "N");
        sendResponse(response, address, port);
    }

    // Process a nearest request by computing distances from the given hex-encoded hash.
    private void processNearestRequest(String transactionId, String hexHash, InetAddress address, int port) {
        try {
            byte[] requestHash = hexStringToByteArray(hexHash);
            List<AddressDistance> distances = new ArrayList<>();
            for (Map.Entry<String, String> entry : addressStore.entrySet()) {
                String key = entry.getKey();
                byte[] keyHash = HashID.computeHashID(key);
                int distance = computeDistance(requestHash, keyHash);
                distances.add(new AddressDistance(key, entry.getValue(), distance));
            }
            distances.sort(Comparator.comparingInt(a -> a.distance));
            StringBuilder responseBuilder = new StringBuilder(transactionId + " O");
            int count = 0;
            for (AddressDistance ad : distances) {
                if (count >= 3) break;
                responseBuilder.append(" ").append(formatAddressPair(ad.nodeName, ad.address));
                count++;
            }
            sendResponse(responseBuilder.toString(), address, port);
            System.out.println("Sent nearest response: " + responseBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Process a key existence request.
    private void processKeyExistenceRequest(String transactionId, String key, InetAddress address, int port) {
        try {
            boolean existsLocal = dataStore.containsKey(key);
            char responseChar = existsLocal ? 'Y' : 'N';
            String response = transactionId + " F " + responseChar;
            sendResponse(response, address, port);
            System.out.println("Sent key existence response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Process a relay message. If the target node is not us, try to forward it.
    private void processRelayMessage(String transactionId, String payload, InetAddress address, int port) {
        try {
            String[] tokens = payload.split(" ", 2);
            if (tokens.length < 2) {
                System.out.println("Invalid relay message format");
                return;
            }
            String targetNode = tokens[0];
            String innerMessage = tokens[1];
            // If this node is the target, process the inner message.
            if (targetNode.equals(nodeName)) {
                if (innerMessage.contains(" G")) {
                    String relayResponse = transactionId + " H " + nodeName;
                    sendResponse(relayResponse, address, port);
                    System.out.println("Processed relay message (name request): " + relayResponse);
                } else {
                    String relayResponse = transactionId + " I Unimplemented relay inner command";
                    sendResponse(relayResponse, address, port);
                    System.out.println("Processed relay message (generic): " + relayResponse);
                }
            } else {
                // Attempt to forward the relay message if we have an address for the target.
                String targetAddressStr = addressStore.get(targetNode);
                if (targetAddressStr != null) {
                    String[] parts = targetAddressStr.split(":");
                    InetAddress targetAddr = InetAddress.getByName(parts[0]);
                    int targetPort = Integer.parseInt(parts[1]);
                    System.out.println("Forwarding relay message to " + targetNode);
                    String response = forwardRelay(innerMessage, targetAddr, targetPort);
                    String relayResponse = transactionId + " " + response;
                    sendResponse(relayResponse, address, port);
                } else {
                    String relayResponse = transactionId + " I Relay target address not found";
                    sendResponse(relayResponse, address, port);
                    System.out.println("Relay forwarding failed: target " + targetNode + " not found");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper method to forward a message and wait for a response.
    private String forwardRelay(String message, InetAddress targetAddr, int targetPort) {
        try {
            String tid = generateTransactionID();
            String request = tid + " " + message;
            sendDirect(request, targetAddr, targetPort);
            long startTime = System.currentTimeMillis();
            int timeout = 5000;
            byte[] buffer = new byte[1024];
            DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
            while (System.currentTimeMillis() - startTime < timeout) {
                try {
                    int remaining = timeout - (int)(System.currentTimeMillis() - startTime);
                    socket.setSoTimeout(remaining);
                    socket.receive(responsePacket);
                    String response = new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
                    String[] tokens = response.split(" ", 2);
                    if (tokens.length >= 2 && tokens[0].equals(tid)) {
                        return tokens[1];
                    }
                } catch (SocketTimeoutException ste) {
                    break;
                }
            }
            return "I Relay response timeout";
        } catch (Exception e) {
            return "I Relay error: " + e.getMessage();
        }
    }

    // Check if a node is active.
    @Override
    public boolean isActive(String nodeName) throws Exception {
        return this.nodeName != null && this.nodeName.equals(nodeName);
    }

    // Push a node name onto the relay stack.
    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
        System.out.println("Pushed relay: " + nodeName);
    }

    // Pop a node name from the relay stack.
    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String removed = relayStack.pop();
            System.out.println("Popped relay: " + removed);
        }
    }

    // Check for the existence of a key in the data store.
    @Override
    public boolean exists(String key) throws Exception {
        return dataStore.containsKey(key);
    }

    private String generateTransactionID() {
        Random random = new Random();
        int r = random.nextInt(0x10000);
        return String.format("%04x", r);
    }

    // Synchronously read the value for a given key.
    @Override
    public String read(String key) throws Exception {
        System.out.println("Initiating read for key: " + key);
        // Check local cache first.
        if (dataStore.containsKey(key)) {
            System.out.println("Key found in local cache.");
            return dataStore.get(key);
        }
        String tid = generateTransactionID();
        String message = tid + " R " + key + " ";
        System.out.println("Sending R request: " + message);

        // Debug: show current addressStore content.
        System.out.println("Current addressStore: " + addressStore);

        // Build target list while excluding own node.
        List<InetSocketAddress> targets = new ArrayList<>();
        for (Map.Entry<String, String> entry : addressStore.entrySet()) {
            if (entry.getKey().equals(this.nodeName)) {
                // Skip our own node.
                continue;
            }
            String addrStr = entry.getValue();
            String[] parts = addrStr.split(":");
            InetAddress addr = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            targets.add(new InetSocketAddress(addr, port));
        }
        int attempts = 0;
        while (attempts < 3) {
            for (InetSocketAddress target : targets) {
                sendRequest(message, target.getAddress(), target.getPort());
            }
            long startTime = System.currentTimeMillis();
            int timeout = 5000;
            byte[] buffer = new byte[1024];
            DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
            while (System.currentTimeMillis() - startTime < timeout) {
                try {
                    int remaining = timeout - (int)(System.currentTimeMillis() - startTime);
                    socket.setSoTimeout(remaining);
                    socket.receive(responsePacket);
                    String response = new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
                    System.out.println("Received response: " + response);
                    String[] tokens = response.split(" ", 4);
                    if (tokens.length >= 4 && tokens[0].equals(tid) && tokens[1].equals("S") && tokens[2].equals("Y")) {
                        dataStore.put(key, tokens[3].trim());
                        return tokens[3].trim();
                    }
                } catch (SocketTimeoutException ste) {
                    break;
                }
            }
            attempts++;
        }
        return null;
    }


    private String localRead(String key) {
        return dataStore.get(key);
    }

    // Write a key/value pair into the local data store.
    @Override
    public boolean write(String key, String value) throws Exception {
        dataStore.put(key, value);
        System.out.println("Written key: " + key + " with value: " + value);
        return true;
    }

    // Perform an atomic compare-and-swap on a key/value pair.
    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        boolean success = dataStore.replace(key, currentValue, newValue);
        System.out.println("CAS on key: " + key + " from '" + currentValue + "' to '" + newValue + "' " + (success ? "succeeded" : "failed"));
        return success;
    }

    // Helper method to send a UDP response directly.
    private void sendResponse(String response, InetAddress address, int port) {
        byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket respPacket = new DatagramPacket(respBytes, respBytes.length, address, port);
        try {
            socket.send(respPacket);
            System.out.println("Sent response: " + response);
        } catch (Exception e) {
            System.err.println("Error sending response: " + e.getMessage());
        }
    }

    // Helper method to send a request taking into account the relay stack.
    private void sendRequest(String message, InetAddress address, int port) throws Exception {
        if (!relayStack.isEmpty()) {
            String relayTarget = relayStack.peek();
            String tid = message.substring(0, 4); // assuming tid is 4 hex digits
            message = tid + " V " + relayTarget + " " + message.substring(5);
        }
        sendDirect(message, address, port);
        System.out.println("Sent packet: " + message + " to " + address.getHostAddress() + ":" + port);
    }

    // Sends a message directly (without relay wrapping).
    private void sendDirect(String message, InetAddress address, int port) throws Exception {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address, port);
        socket.send(packet);
        System.out.println("Sent packet: " + message + " to " + address.getHostAddress() + ":" + port);
    }

    // Compute the "distance" between two hashIDs as defined in the RFC.
    private int computeDistance(byte[] hash1, byte[] hash2) {
        int matchingBits = 0;
        for (int i = 0; i < Math.min(hash1.length, hash2.length); i++) {
            int xor = hash1[i] ^ hash2[i];
            for (int j = 7; j >= 0; j--) {
                if ((xor & (1 << j)) == 0) {
                    matchingBits++;
                } else {
                    return 256 - matchingBits;
                }
            }
        }
        return 256 - matchingBits;
    }

    // Convert a hex string to a byte array.
    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte)((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    // Format an address pair as per protocol (e.g., "0 N:test 0 127.0.0.1:20110").
    private String formatAddressPair(String nodeName, String address) {
        return "0 " + nodeName + " 0 " + address;
    }

    // Helper class for storing address distances.
    private static class AddressDistance {
        String nodeName;
        String address;
        int distance;
        AddressDistance(String nodeName, String address, int distance) {
            this.nodeName = nodeName;
            this.address = address;
            this.distance = distance;
        }
    }
}
