import java.net.InetAddress;

public class AzureLabTest {
    public static void main(String[] args) {
        try {
            // Automatically detect your VM's actual IP address.
            String ipAddress = InetAddress.getLocalHost().getHostAddress();
            System.out.println("Detected VM IP: " + ipAddress);

            // Replace with your actual email address.
            String emailAddress = "zidan.ahmed.2@city.ac.uk";

            // Number of nodes to simulate.
            int numNodes = 3;
            Node[] nodes = new Node[numNodes];

            // Create and initialize each node with a unique name and port.
            for (int i = 0; i < numNodes; i++) {
                nodes[i] = new Node();
                // Each node gets a unique name by appending an index.
                String nodeName = "N:" + emailAddress + "-" + i;
                nodes[i].setNodeName(nodeName);
                // Use ports in the allowed range, e.g., 20110, 20111, 20112.
                nodes[i].openPort(20110 + i);
            }

            // Start periodic active mapping on each node (every 5000 ms).
            for (int i = 0; i < numNodes; i++) {
                nodes[i].startPeriodicActiveMapping(5000);
            }

            // Start handling incoming messages for each node concurrently.
            for (int i = 0; i < numNodes; i++) {
                final int index = i;
                new Thread(() -> {
                    try {
                        nodes[index].handleIncomingMessages(0); // 0 means wait indefinitely.
                    } catch (Exception e) {
                        System.err.println("Exception in node " + index);
                        e.printStackTrace();
                    }
                }).start();
            }

            // Allow time for nodes to bootstrap and exchange addresses.
            System.out.println("Waiting for nodes to bootstrap...");
            Thread.sleep(10000);

            // Example operation: Node 0 writes a key/value pair.
            boolean writeSuccess = nodes[0].write("D:sampleKey", "Hello from node0!");
            System.out.println("Node 0 wrote key D:sampleKey with value 'Hello from node0!'");
            Thread.sleep(2000);

            // Attempt to read the key from Node 1.
            String value = nodes[1].read("D:sampleKey");
            if (value != null) {
                System.out.println("Node 1 read key D:sampleKey: " + value);
            } else {
                System.err.println("Node 1 could not read key D:sampleKey.");
            }

            // Announce each node's address so that other nodes can contact them.
            for (int i = 0; i < numNodes; i++) {
                String nodeName = "N:" + emailAddress + "-" + i;
                String nodeAddress = ipAddress + ":" + (20110 + i);
                System.out.println("Announcing node " + i + " address: " + nodeAddress);
                nodes[i].write(nodeName, nodeAddress);
            }

            // Keep the program running indefinitely.
            System.out.println("Nodes are running. Press Ctrl+C to exit.");
            while (true) {
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
