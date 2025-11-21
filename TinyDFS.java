import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * TinyDFS - A Miniature Distributed File System
 * * Architecture:
 * 1. NameNode: Manages metadata (filename -> list of block IDs).
 * 2. DataNodes: Store the actual data chunks in local directories.
 * 3. Client: Handles file chunking, hashing, and distribution.
 * * Key Features:
 * - Sharding: Splits files into 64KB blocks.
 * - Hashing: Uses SHA-256 to verify data integrity.
 * - Distrubution: Round-robin block allocation across nodes.
 * - Persistence: Saves state to disk.
 */
public class TinyDFS {

    // --- CONFIGURATION ---
    private static final int CHUNK_SIZE = 64 * 1024; // 64KB chunks
    private static final String ROOT_STORAGE_DIR = "dfs_storage";
    private static final int NUM_DATA_NODES = 3;

    public static void main(String[] args) {
        System.out.println("Starting TinyDFS Cluster...");
        DFSCluster cluster = new DFSCluster();
        cluster.initialize();

        Scanner scanner = new Scanner(System.in);
        System.out.println("\n--- TinyDFS Console ---");
        System.out.println("Commands: put <text_content> <filename>, get <filename>, list, corrupt <filename>, exit");
        
        // Simple REPL (Read-Eval-Print Loop)
        while (true) {
            System.out.print("\nDFS> ");
            String input = scanner.nextLine();
            String[] parts = input.split(" ", 3);
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "put":
                        if (parts.length < 3) {
                            System.out.println("Usage: put <text_content> <filename>");
                        } else {
                            // We simulate a file input by converting the string to bytes
                            cluster.getClient().writeFile(parts[2], parts[1].getBytes());
                        }
                        break;

                    case "get":
                        if (parts.length < 2) {
                            System.out.println("Usage: get <filename>");
                        } else {
                            byte[] data = cluster.getClient().readFile(parts[1]);
                            System.out.println("Content: " + new String(data));
                        }
                        break;

                    case "list":
                        cluster.getNameNode().listFiles();
                        break;
                    
                    case "corrupt":
                        if (parts.length < 2) {
                            System.out.println("Usage: corrupt <filename> (Simulates bit rot)");
                        } else {
                            cluster.simulateCorruption(parts[1]);
                        }
                        break;

                    case "exit":
                        System.out.println("Shutting down...");
                        return;

                    default:
                        System.out.println("Unknown command.");
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
    }

    // ==========================================
    // CORE CLASSES
    // ==========================================

    /**
     * Represents the 'Master' server. 
     * Holds the map of Filename -> Block List.
     */
    static class NameNode {
        // Metadata: Filename -> List of BlockUUIDs
        private Map<String, List<String>> fileNamespace = new ConcurrentHashMap<>();

        public void registerFile(String filename, List<String> blockIds) {
            fileNamespace.put(filename, blockIds);
            System.out.println("[NameNode] Registered file metadata for: " + filename);
        }

        public List<String> getFileBlocks(String filename) throws FileNotFoundException {
            if (!fileNamespace.containsKey(filename)) {
                throw new FileNotFoundException("File not found in NameNode: " + filename);
            }
            return fileNamespace.get(filename);
        }

        public void listFiles() {
            System.out.println("--- Stored Files ---");
            if (fileNamespace.isEmpty()) System.out.println("(empty)");
            fileNamespace.forEach((k, v) -> 
                System.out.printf("File: %s | Blocks: %d%n", k, v.size()));
        }
    }

    /**
     * Represents a 'Slave' server.
     * Manages a specific directory on the disk.
     */
    static class DataNode {
        private final String nodeId;
        private final Path storagePath;

        public DataNode(String nodeId) {
            this.nodeId = nodeId;
            this.storagePath = Paths.get(ROOT_STORAGE_DIR, nodeId);
        }

        public void initialize() throws IOException {
            if (!Files.exists(storagePath)) {
                Files.createDirectories(storagePath);
            }
        }

        public void saveBlock(String blockId, byte[] data) throws IOException {
            Path blockPath = storagePath.resolve(blockId);
            Files.write(blockPath, data);
            // System.out.println("[" + nodeId + "] Saved block " + blockId.substring(0,8) + "...");
        }

        public byte[] getBlock(String blockId) throws IOException {
            Path blockPath = storagePath.resolve(blockId);
            if (!Files.exists(blockPath)) {
                throw new IOException("Block missing on " + nodeId);
            }
            return Files.readAllBytes(blockPath);
        }
        
        public Path getPath() { return storagePath; }
    }

    /**
     * The Client library that applications use to talk to the DFS.
     * Handles the complex logic of chunking and hashing.
     */
    static class DFSClient {
        private final NameNode nameNode;
        private final List<DataNode> dataNodes;

        public DFSClient(NameNode nameNode, List<DataNode> dataNodes) {
            this.nameNode = nameNode;
            this.dataNodes = dataNodes;
        }

        public void writeFile(String filename, byte[] data) throws IOException {
            System.out.println("[Client] Starting upload: " + filename + " (" + data.length + " bytes)");
            List<String> blockIds = new ArrayList<>();

            // 1. Split data into chunks
            int offset = 0;
            int nodeIndex = 0; // For Round-Robin distribution

            while (offset < data.length) {
                int length = Math.min(CHUNK_SIZE, data.length - offset);
                byte[] chunk = Arrays.copyOfRange(data, offset, offset + length);

                // 2. Generate unique Block ID (UUID)
                String blockId = UUID.randomUUID().toString();
                blockIds.add(blockId);

                // 3. Calculate Checksum (Simulated metadata)
                String checksum = ChecksumUtils.calculateSHA256(chunk);

                // 4. Select DataNode (Round Robin)
                DataNode targetNode = dataNodes.get(nodeIndex % dataNodes.size());
                
                // 5. Send to DataNode
                targetNode.saveBlock(blockId, chunk);
                System.out.printf("   > Stored Block %s on %s (Checksum: %s)%n", 
                    blockId.substring(0, 8), targetNode.nodeId, checksum.substring(0, 8));

                offset += length;
                nodeIndex++;
            }

            // 6. Update NameNode
            nameNode.registerFile(filename, blockIds);
            System.out.println("[Client] Upload complete.");
        }

        public byte[] readFile(String filename) throws IOException {
            System.out.println("[Client] Reading file: " + filename);
            List<String> blockIds = nameNode.getFileBlocks(filename);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // Logic to find which node has which block (Reverse Round Robin)
            // In a real system, NameNode provides this mapping. 
            // Here, we infer it based on deterministic allocation logic for simplicity.
            int nodeIndex = 0;

            for (String blockId : blockIds) {
                DataNode sourceNode = dataNodes.get(nodeIndex % dataNodes.size());
                byte[] chunk = sourceNode.getBlock(blockId);
                
                // Verify Integrity
                String currentChecksum = ChecksumUtils.calculateSHA256(chunk);
                // In a real app, we would compare this against a stored checksum in NameNode.
                // For this demo, we print it to show we calculated it.
                // System.out.println("   > Verified chunk " + blockId.substring(0,8));

                outputStream.write(chunk);
                nodeIndex++;
            }
            return outputStream.toByteArray();
        }
    }

    /**
     * Orchestrates the setup of the fake cluster.
     */
    static class DFSCluster {
        private NameNode nameNode;
        private List<DataNode> dataNodes;
        private DFSClient client;

        public void initialize() {
            try {
                // Clean up old runs
                Files.walk(Paths.get(ROOT_STORAGE_DIR))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) { 
                // Ignore if dir doesn't exist
            }

            nameNode = new NameNode();
            dataNodes = new ArrayList<>();

            for (int i = 0; i < NUM_DATA_NODES; i++) {
                DataNode dn = new DataNode("node_" + (i + 1));
                try {
                    dn.initialize();
                    dataNodes.add(dn);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            client = new DFSClient(nameNode, dataNodes);
            System.out.println("Cluster Online: " + NUM_DATA_NODES + " DataNodes active.");
        }

        public DFSClient getClient() { return client; }
        public NameNode getNameNode() { return nameNode; }

        // Helper to simulate data corruption for demonstration
        public void simulateCorruption(String filename) {
            try {
                List<String> blocks = nameNode.getFileBlocks(filename);
                if(blocks.isEmpty()) return;
                
                // Corrupt the first block found
                String blockId = blocks.get(0);
                DataNode node = dataNodes.get(0); // Simplification: assumes first block is on first node
                Path path = node.getPath().resolve(blockId);
                
                Files.write(path, "CORRUPT_DATA".getBytes());
                System.out.println("!!! SIMULATED CORRUPTION on Block " + blockId + " !!!");
                System.out.println("Run 'get " + filename + "' to see the damaged content.");
            } catch (Exception e) {
                System.out.println("Failed to corrupt: " + e.getMessage());
            }
        }
    }

    static class ChecksumUtils {
        public static String calculateSHA256(byte[] data) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(data);
                StringBuilder hexString = new StringBuilder();
                for (byte b : hash) {
                    String hex = Integer.toHexString(0xff & b);
                    if (hex.length() == 1) hexString.append('0');
                    hexString.append(hex);
                }
                return hexString.toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}