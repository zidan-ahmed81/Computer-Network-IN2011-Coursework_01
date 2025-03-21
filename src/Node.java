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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.*;

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
    private DatagramSocket socket;
    private int port;

    private final Map<String, String> addressStore = new HashMap<>();
    private byte[] myHashID;
    private final Stack<String> relayStack = new Stack<>();
    private final Map<Integer, List<Map.Entry<String, String>>> addressByDistance = new HashMap<>();

    @Override
    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
        this.socket = new DatagramSocket(portNumber);

        String address = "127.0.0.1:" + portNumber;
        addressStore.put(nodeName, address);
        myHashID = HashID.computeHashID(nodeName);
        System.out.println("Stored own address: " + nodeName + " → " + address);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        byte[] buffer = new byte[2048];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        if (delay == 0) {
            socket.setSoTimeout(0);
        } else {
            socket.setSoTimeout(delay);
        }

        try {
            socket.receive(packet);
            String msg = new String(packet.getData(), 0, packet.getLength(), "UTF-8");
            System.out.println("Received: " + msg);
            processMessage(msg, packet.getAddress(), packet.getPort());
        } catch (SocketTimeoutException e) {
            // timeout
        }
    }

    private void processMessage(String msg, InetAddress senderAddress, int senderPort) throws Exception {
        String[] parts = msg.split(" ", 3);
        if (parts.length < 2) return;

        String txID = parts[0];
        String type = parts[1];

        if (type.equals("G")) {
            String response = txID + " H " + nodeName + " ";
            byte[] data = response.getBytes("UTF-8");
            DatagramPacket responsePacket = new DatagramPacket(data, data.length, senderAddress, senderPort);
            socket.send(responsePacket);
            System.out.println("Replied with: " + response);

        } else if (type.equals("H")) {
            System.out.println("Received name response: " + msg);

        } else if (type.equals("W")) {
            String[] kvParts = parts[2].split(" ", 4);
            if (kvParts.length < 4) return;

            String key = kvParts[1];
            String value = kvParts[3];

            if (!key.startsWith("N:")) return;

            byte[] keyHash = HashID.computeHashID(key);
            int dist = hashDistance(myHashID, keyHash);

            List<Map.Entry<String, String>> list = addressByDistance.getOrDefault(dist, new ArrayList<>());
            boolean alreadyPresent = list.stream().anyMatch(e -> e.getKey().equals(key));

            if (!alreadyPresent && list.size() < 3) {
                list.add(Map.entry(key, value));
                addressByDistance.put(dist, list);
                addressStore.put(key, value);
                System.out.println("Stored address key: " + key + " → " + value + " at distance " + dist);
            }

            String response = parts[0] + " X A";
            sendMessage(response, senderAddress, senderPort);
        }
    }

    private int hashDistance(byte[] a, byte[] b) {
        int matchingBits = 0;
        for (int i = 0; i < a.length; i++) {
            int xor = (a[i] ^ b[i]) & 0xFF;
            for (int bit = 7; bit >= 0; bit--) {
                if ((xor & (1 << bit)) == 0) {
                    matchingBits++;
                } else {
                    return 256 - matchingBits;
                }
            }
        }
        return 0;
    }

    private String generateTxID() {
        Random rand = new Random();
        char a = (char) ('A' + rand.nextInt(26));
        char b = (char) ('A' + rand.nextInt(26));
        return "" + a + b;
    }

    private void sendMessage(String msg, InetAddress ip, int port) throws Exception {
        byte[] data = msg.getBytes("UTF-8");
        DatagramPacket packet = new DatagramPacket(data, data.length, ip, port);
        socket.send(packet);
    }

    @Override
    public boolean isActive(String targetNodeName) throws Exception {
        String address = addressStore.get(targetNodeName);
        if (address == null) return false;

        String[] parts = address.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int targetPort = Integer.parseInt(parts[1]);

        String txID = generateTxID();
        String request = txID + " G";
        sendMessage(request, ip, targetPort);

        socket.setSoTimeout(5000);
        byte[] buffer = new byte[2048];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        try {
            socket.receive(packet);
            String response = new String(packet.getData(), 0, packet.getLength(), "UTF-8");
            return response.startsWith(txID + " H " + targetNodeName);
        } catch (SocketTimeoutException e) {
            return false;
        }
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



    public boolean exists(String key) throws Exception {
	throw new Exception("Not implemented");
    }
    
    public String read(String key) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean write(String key, String value) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
	throw new Exception("Not implemented");
    }
}
