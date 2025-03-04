# Distributed Key-Value Store

A simple distributed key-value store with primary-backup replication and health monitoring.

## Components

- **Client**: Sends write requests to the primary server
- **Primary Server**: Handles client requests and forwards to backup
- **Backup Server**: Stores replicated data
- **Heartbeat Service**: Monitors server health

## Running the System

**IMPORTANT**: Start components in this order:

1. **Heartbeat Service**:
```bash
python heartbeat_service.py
```

2. **Backup Server**:
```bash
python backup.py
```

3. **Primary Server**:
```bash
python primary.py
```

4. **Client**:
```bash
python client.py
```

## How It Works

- Client sends write requests to primary
- Primary forwards writes to backup
- Backup acknowledges and stores data
- Primary then stores data and responds to client
- All servers send heartbeats to monitor health
- Each component maintains its own data file

