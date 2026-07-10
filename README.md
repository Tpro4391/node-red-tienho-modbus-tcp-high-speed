# node-red-tienho-modbus-tcp-high-speed

A high-speed, robust realtime Modbus TCP/IP reader node for Node-RED featuring an integrated **Connection Pool** and **Batch Processing** support.

This node acts as a Modbus TCP Master (Client) to read coils, inputs, and registers. It is optimized for industrial automation where high-frequency polling and multi-device connection management are critical.

---

## Key Features in v2.0.0

*   🚀 **High-Speed Connection Pool**: TCP connections are cached and reused by `ip:port`. Sockets automatically close after a **10-second idle timeout** to prevent PLC connection overload.
*   ⚡ **Concurrency & Parallel Execution**: Completely free of global state. Multiple concurrent messages or multiple instances in a flow execute independently without crosstalk or socket interference.
*   📦 **Batch Processing (Array Input)**: Pass an array of multiple request configurations in `msg.payload`. The node queries all devices in parallel using `Promise.all` and returns a corresponding array of results.
*   🎯 **Standardized Output Format**: Both single and batch requests return detailed objects containing connection metadata, status (`success` or `error`), raw response buffers, errors, and response duration times in `msg.payload`.
*   🛡️ **Robust Error Handling**: Connection failures or device exceptions do not halt your flow. The error state is safely encapsulated in the output payload, and standard Node-RED Catch Nodes are supported.

---

## Installation

Run the following command in your Node-RED user directory (usually `~/.node-red`):

```bash
npm install node-red-tienho-modbus-tcp-high-speed
```

---

## Input Message Structure

You can configure the connection statically in the Node UI or pass it dynamically in `msg.payload`.

### 1. Single Request Format (Object)
Pass a single object inside `msg.payload`:

```json
{
    "payload": {
        "functioncode": 3,
        "address": 1000,
        "quantity": 10,
        "unitid": 1,
        "modbus_ip": "192.168.1.10",    // Optional (falls back to UI config if omitted)
        "modbus_port": 502              // Optional (falls back to UI config if omitted)
    }
}
```

### 2. Batch Request Format (Array)
Pass an array of objects inside `msg.payload` to read from different servers/registers concurrently:

```json
{
    "payload": [
        {
            "modbus_ip": "192.168.1.10",
            "modbus_port": 502,
            "functioncode": 3,
            "address": 100,
            "quantity": 2,
            "unitid": 1
        },
        {
            "modbus_ip": "192.168.1.20",
            "modbus_port": 502,
            "functioncode": 3,
            "address": 200,
            "quantity": 5,
            "unitid": 1
        }
    ]
}
```

### Modbus Function Codes Supported

This node supports both **Read** and **Write** operations:

#### Read Operations
*   `1`: Read Coils (FC 01) — Requires `quantity`
*   `2`: Read Discrete Inputs (FC 02) — Requires `quantity`
*   `3`: Read Holding Registers (FC 03) — Requires `quantity`
*   `4`: Read Input Registers (FC 04) — Requires `quantity`

#### Write Operations (with Smart Data Parsing)
*   `5`: Write Single Coil (FC 05) — Requires `value` (accepts boolean `true`/`false` or `1`/`0`)
*   `6`: Write Single Register (FC 06) — Requires `value` (accepts a number e.g. `123`, automatically converted to 2-byte Buffer)
*   `15`: Write Multiple Coils (FC 15) — Requires `values` (accepts array e.g. `[true, false, true]`)
*   `16`: Write Multiple Registers (FC 16) — Requires `values` (accepts array of numbers e.g. `[100, 200]`, automatically converted to Buffer)

---

## Output Message Structure

The results are always assigned directly to `msg.payload` with a comprehensive output schema.

### 1. Successful Query Result Example
```json
{
    "payload": {
        "ip": "192.168.1.10",
        "port": 502,
        "address": 1000,
        "quantity": 10,
        "unitid": 1,
        "functioncode": 3,
        "status": "success",
        "buffer": [25, 218, 80, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "error": null,
        "durationMs": 15
    }
}
```

### 2. Failed Query Result Example
```json
{
    "payload": {
        "ip": "192.168.1.33",
        "port": 502,
        "address": 0,
        "quantity": 10,
        "unitid": 1,
        "functioncode": 3,
        "status": "error",
        "buffer": null,
        "error": "Connection error: Connection refused"
    }
}
```

*Note: For backward compatibility with v1.x, single requests will also populate `msg.responseBuffer = { buffer: rawBuffer }`, `msg.ip`, and `msg.port` on successful reads.*

---

## License

This project is licensed under the MIT License.
