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
import java.util.concurrent.*;
import java.util.*;
import java.security.MessageDigest;

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
    private String nodeName;
    private byte[] nodeHash;
    private DatagramSocket socket;

    // A simple key/value store for data pairs (for keys starting with "D:")
    private ConcurrentMap<String, String> dataStore = new ConcurrentHashMap<>();

    // A stack to maintain relay nodes
    private Deque<String> relayStack = new ConcurrentLinkedDeque<>();

    // Add this field to your Node class (at the top)
    private ConcurrentMap<String, String> addressStore = new ConcurrentHashMap<>();

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
        // Store your own address key/value pair (using your node name as key)
        String address = InetAddress.getLocalHost().getHostAddress() + ":" + portNumber;
        if (nodeName != null) {
            addressStore.put(nodeName, address);
        }
        System.out.println("Opened UDP port " + portNumber);
    }


    // This method listens for incoming UDP messages for a specified delay (in milliseconds).
    // It parses each message and dispatches the handling based on the command.
    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        // Set a timeout if delay > 0, otherwise block indefinitely.
        if (delay > 0) {
            socket.setSoTimeout(delay);
        } else {
            socket.setSoTimeout(0);
        }
        try {
            while (true) {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                System.out.println("Received: " + message);
                processMessage(message, packet.getAddress(), packet.getPort());
            }
        } catch (SocketTimeoutException ste) {
            // No message received within the delay period; exit the method.
        }
    }

    // Parse a received message and dispatch to the appropriate handler.
    // We assume a simplified protocol format: "<TID> <command> <payload>"
    private void processMessage(String message, InetAddress address, int port) {
        String[] parts = message.split(" ", 3);
        if (parts.length < 2) {
            System.out.println("Invalid message format");
            return;
        }
        String transactionId = parts[0];
        String command = parts[1];
        String payload = (parts.length >= 3) ? parts[2] : "";

        switch (command) {
            case "G": // Name request: reply with our node name.
                sendNameResponse(transactionId, address, port);
                break;
            case "R": // Read request: payload contains the key.
                sendReadResponse(transactionId, payload, address, port);
                break;
            case "W": // Write request: payload contains key and value separated by a space.
                String[] kv = payload.split(" ", 2);
                if (kv.length == 2) {
                    boolean writeResult = false;
                    try {
                        writeResult = write(kv[0], kv[1]);
                    } catch (Exception e) {
                        System.err.println("Error in write: " + e.getMessage());
                    }
                    sendWriteResponse(transactionId, writeResult, address, port);
                }
                break;
            case "C": // Compare And Swap (CAS) request: payload has key, currentValue, newValue.
                String[] partsCAS = payload.split(" ", 3);
                if (partsCAS.length == 3) {
                    boolean casResult = false;
                    try {
                        casResult = CAS(partsCAS[0], partsCAS[1], partsCAS[2]);
                    } catch (Exception e) {
                        System.err.println("Error in CAS: " + e.getMessage());
                    }
                    sendCASResponse(transactionId, casResult, address, port);
                }
                break;
            case "N": // Nearest Request
                processNearestRequest(transactionId, payload, address, port);
                break;
            case "E": // Key Existence Request
                processKeyExistenceRequest(transactionId, payload, address, port);
                break;
            case "V": // Relay message
                processRelayMessage(transactionId, payload, address, port);
                break;
            default:
                System.out.println("Unknown command: " + command);
        }
    }

    private void processNearestRequest(String transactionId, String hexHash, InetAddress address, int port) {
        try {
            byte[] requestHash = hexStringToByteArray(hexHash);

            // Create a list to hold address pairs with their computed distance
            List<AddressDistance> distances = new ArrayList<>();
            for (Map.Entry<String, String> entry : addressStore.entrySet()) {
                String nodeKey = entry.getKey();
                byte[] nodeHash = HashID.computeHashID(nodeKey);
                int distance = computeDistance(requestHash, nodeHash);
                distances.add(new AddressDistance(nodeKey, entry.getValue(), distance));
            }

            // Sort the list by distance (lowest distance first)
            distances.sort((a, b) -> Integer.compare(a.distance, b.distance));

            // Build the response string with up to three address pairs
            StringBuilder responseBuilder = new StringBuilder(transactionId + " O");
            int count = 0;
            for (AddressDistance ad : distances) {
                if (count >= 3) break;
                responseBuilder.append(" ").append(formatAddressPair(ad.nodeName, ad.address));
                count++;
            }

            String response = responseBuilder.toString();
            sendResponse(response, address, port);
            System.out.println("Sent nearest response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper: Convert a hex string to a byte array.
    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    // Helper: Compute distance between two hashIDs
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

    // Helper: Format an address pair according to the protocol.
// For example: "0 N:test 0 127.0.0.1:20110"
    private String formatAddressPair(String nodeName, String address) {
        return "0 " + nodeName + " 0 " + address;
    }

    // Helper class to store address distances.
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

    // (Optional) Helper: Convert a byte array to a hex string.
    private String byteArrayToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private void processKeyExistenceRequest(String transactionId, String key, InetAddress address, int port) {
        try {
            boolean existsLocal = dataStore.containsKey(key);
            // For now, assume this node is among the three closest nodes.
            boolean isNearest = true;

            char responseChar;
            if (existsLocal) {
                responseChar = 'Y';  // Key exists
            } else if (!existsLocal && isNearest) {
                responseChar = 'N';  // Key doesn't exist, but node is one of the nearest
            } else {
                responseChar = '?';  // Not one of the nearest
            }

            String response = transactionId + " F " + responseChar;
            sendResponse(response, address, port);
            System.out.println("Sent key existence response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processRelayMessage(String transactionId, String payload, InetAddress address, int port) {
        try {
            // Split the payload into the target node and the inner message.
            String[] tokens = payload.split(" ", 2);
            if (tokens.length < 2) {
                System.out.println("Invalid relay message format");
                return;
            }
            String targetNode = tokens[0];
            String innerMessage = tokens[1];

            // For our simple test, if the target node is this node, process the inner message locally.
            if (targetNode.equals(nodeName)) {
                // For a name request, innerMessage should contain " G"
                if (innerMessage.contains(" G")) {
                    // Create a name response using the relay's transaction ID.
                    String relayResponse = transactionId + " H " + nodeName;
                    sendResponse(relayResponse, address, port);
                    System.out.println("Processed relay message (name request): " + relayResponse);
                } else {
                    // For other inner messages, you can implement further handling.
                    String relayResponse = transactionId + " I Unimplemented relay inner command";
                    sendResponse(relayResponse, address, port);
                    System.out.println("Processed relay message (generic): " + relayResponse);
                }
            } else {
                // If the target node is not this node, for now we do not implement forwarding.
                String relayResponse = transactionId + " I Relay forwarding not implemented";
                sendResponse(relayResponse, address, port);
                System.out.println("Relay forwarding not implemented: " + relayResponse);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // Send a name response message in reply to a name request.
    private void sendNameResponse(String transactionId, InetAddress address, int port) {
        String response = transactionId + " H " + nodeName;
        sendResponse(response, address, port);
    }

    // Send a read response. If the key exists, respond with "Y" and the value; otherwise "N".
    private void sendReadResponse(String transactionId, String key, InetAddress address, int port) {
        String value = null;
        try {
            value = read(key);
        } catch (Exception e) {
            System.err.println("Error in read: " + e.getMessage());
        }
        String response;
        if (value != null) {
            response = transactionId + " S Y " + value;
        } else {
            response = transactionId + " S N";
        }
        sendResponse(response, address, port);
    }

    // Send a write response message. In this simplified version, we always return a success code.
    private void sendWriteResponse(String transactionId, boolean success, InetAddress address, int port) {
        // For simplicity, we return "A" for a new value or replacement.
        String response = transactionId + " X " + (success ? "A" : "X");
        sendResponse(response, address, port);
    }

    // Send a CAS response message. Return "R" if successful, "N" otherwise.
    private void sendCASResponse(String transactionId, boolean success, InetAddress address, int port) {
        String response = transactionId + " D " + (success ? "R" : "N");
        sendResponse(response, address, port);
    }

    // Helper method to send a UDP response.
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

    // Check if a node is active.
    // In this simple implementation, we return true if the provided name matches our node.
    @Override
    public boolean isActive(String nodeName) throws Exception {
        return this.nodeName != null && this.nodeName.equals(nodeName);
    }

    // Add a node name to the relay stack.
    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
        System.out.println("Pushed relay: " + nodeName);
    }

    // Remove the top node name from the relay stack.
    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String removed = relayStack.pop();
            System.out.println("Popped relay: " + removed);
        }
    }

    // Check if a given key exists in the data store.
    @Override
    public boolean exists(String key) throws Exception {
        return dataStore.containsKey(key);
    }

    // Read the value associated with a key.
    @Override
    public String read(String key) throws Exception {
        return dataStore.get(key);
    }

    // Write a key/value pair into the data store.
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
}
