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
            int numNodes = 6;
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
                        // 0 means wait indefinitely for messages.
                        nodes[index].handleIncomingMessages(0);
                    } catch (Exception e) {
                        System.err.println("Exception in node " + index);
                        e.printStackTrace();
                    }
                }).start();
            }

            // Allow time for nodes to bootstrap and exchange addresses.
            System.out.println("Waiting for nodes to bootstrap...");
            Thread.sleep(10000);

            // Write the poem verses from Node0.
            String[] poemVerses = {
                    "’Twas brillig, and the slithy toves",
                    "Did gyre and gimble in the wabe;",
                    "All mimsy were the borogoves,",
                    "And the mome raths outgrabe.",
                    "Beware the Jabberwock, my son!",
                    "The jaws that bite, the claws that catch!",
                    "Beware the Jubjub bird, and shun"
            };

            for (int i = 0; i < poemVerses.length; i++) {
                String key = "D:jabberwocky" + i;
                boolean success = nodes[0].write(key, poemVerses[i]);
                System.out.println("Node 0 wrote " + key + " with value: " + poemVerses[i]);
                Thread.sleep(500); // slight delay between writes
            }

            // Allow time for propagation.
            Thread.sleep(2000);

            // Attempt to read the poem verses from Node1.
            System.out.println("Node 1 attempting to read poem verses:");
            for (int i = 0; i < poemVerses.length; i++) {
                String key = "D:jabberwocky" + i;
                String verse = nodes[1].readFrom(key, "10.200.51.19", 20114);
                if (verse != null) {
                    System.out.println("Node 1 read " + key + ": " + verse);
                } else {
                    System.err.println("Node 1 could not read " + key);
                }
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
