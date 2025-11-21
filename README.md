# TinyDFS - Distributed File System (Java)

A lightweight, educational Distributed File System implemented in pure Java.  
It simulates file chunking, block distribution, metadata management, and integrity handling similar to Google GFS and Hadoop HDFS.

## Features

- **File Chunking**: Splits files into 64KB blocks
- **Distributed Storage**: Stores blocks across multiple simulated DataNodes
- **SHA-256 Integrity**: Each block is hashed for verification
- **Metadata Management**: NameNode tracks filenames and block IDs
- **Console Interface**: Commands to store, list, retrieve, and corrupt files
- **Local Simulation**: Entire DFS runs locally without networking

## Getting Started

### Prerequisites
- Java Development Kit (JDK) 8 or higher

### Installation
1. Clone this repository  
2. Navigate to the project directory

### Running the Application
Compile:
```
javac TinyDFS.java
```

Run:
```
java TinyDFS
```

Use the console commands shown below.

## Available Commands

Store a file:
```
put "your content here" filename.txt
```

List files:
```
list
```

Retrieve a file:
```
get filename.txt
```

Simulate corruption:
```
corrupt filename.txt
```

## Project Structure

- `TinyDFS.java` — Core logic (NameNode, DataNode, DFS Client)
- `dfs_storage/` — Auto-created storage directory
- `node_1/`, `node_2/`, etc. — DataNode block storage folders
- `README.md` — Documentation

## How It Works

### Write Path
Input → Split 64KB blocks → Hash with SHA-256 → Round-robin node assignment → Save to disk

### Read Path
Metadata lookup → Fetch blocks → Reassemble file → Output

### Metadata
Uses:
```
ConcurrentHashMap<String, List<UUID>>
```

## Technologies Used

- Java SE (JDK 8+)
- SHA-256 hashing
- Local file system for DataNode simulation
- ConcurrentHashMap for metadata

## Future Improvements

- Block replication
- DataNode heartbeat system
- Java Socket-based networking
- Automatic integrity verification

## License
Open source for educational and personal use.

## Acknowledgements
Inspired by concepts from Google File System (GFS) and Hadoop HDFS, simplified for learning.
