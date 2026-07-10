const modbus = require("./module/modbus-stream/lib/modbus");

module.exports = function (RED) {
    function ModbusTcpIpNode(config) {
        RED.nodes.createNode(this, config);
        this.ip = config.ip;
        this.port = config.port;
        this.logerror = config.logerror;
        var node = this;

        // Initialize connection pool for this node instance to avoid global state pollution
        node.connectionPool = new Map();

        // Helper to get or create connection
        const getModbusConnection = (ip, port) => {
            const key = `${ip}:${port}`;
            if (node.connectionPool.has(key)) {
                const poolEntry = node.connectionPool.get(key);
                if (poolEntry.connection && !poolEntry.connection.destroyed) {
                    if (poolEntry.idleTimer) {
                        clearTimeout(poolEntry.idleTimer);
                        poolEntry.idleTimer = null;
                    }
                    poolEntry.refCount++;
                    return Promise.resolve(poolEntry.connection);
                }
            }

            return new Promise((resolve, reject) => {
                node.status({
                    fill: "yellow",
                    shape: "dot",
                    text: `Connecting to ${ip}:${port}`
                });

                modbus.tcp.connect(port, ip, {
                    debug: null,
                    connectTimeout: 3000
                }, (err, connection) => {
                    if (err) {
                        node.status({
                            fill: "red",
                            shape: "dot",
                            text: `Conn Error: ${err.message}`
                        });
                        return reject(err);
                    }

                    const poolEntry = {
                        connection: connection,
                        refCount: 1,
                        idleTimer: null
                    };
                    node.connectionPool.set(key, poolEntry);

                    const cleanup = () => {
                        if (node.connectionPool.has(key)) {
                            const entry = node.connectionPool.get(key);
                            if (entry.idleTimer) clearTimeout(entry.idleTimer);
                            node.connectionPool.delete(key);
                            node.status({
                                fill: "red",
                                shape: "dot",
                                text: `Disconnected: ${key}`
                            });
                        }
                    };

                    connection.on('error', (error) => {
                        if (node.logerror) {
                            node.error(`Socket error on ${ip}:${port} - ${error.message}`);
                        }
                        cleanup();
                    });

                    connection.on('close', () => {
                        cleanup();
                    });

                    resolve(connection);
                });
            });
        };

        // Helper to release connection back to pool
        const releaseConnection = (ip, port) => {
            const key = `${ip}:${port}`;
            if (node.connectionPool.has(key)) {
                const poolEntry = node.connectionPool.get(key);
                poolEntry.refCount--;
                if (poolEntry.refCount <= 0) {
                    poolEntry.refCount = 0;
                    poolEntry.idleTimer = setTimeout(() => {
                        if (node.connectionPool.has(key)) {
                            const currentEntry = node.connectionPool.get(key);
                            if (currentEntry.refCount === 0) {
                                if (currentEntry.connection) {
                                    try {
                                        currentEntry.connection.close(() => {
                                            node.log(`Connection to ${key} closed due to idle timeout`);
                                        });
                                    } catch (e) {}
                                }
                                node.connectionPool.delete(key);
                            }
                        }
                    }, 10000); // 10s timeout
                }
            }
        };

        // Helper to execute single query
        const executeQuery = (request, ipDefault, portDefault) => {
            const ip = request.ip || request.modbus_ip || ipDefault;
            const port = request.port || request.modbus_port || portDefault;

            const result = {
                ip: ip,
                port: port,
                address: request.address,
                quantity: request.quantity,
                unitid: request.unitid,
                functioncode: request.functioncode,
                status: "error",
                buffer: null,
                error: null
            };

            if (!ip || !port) {
                result.error = "Invalid Modbus IP or PORT";
                return Promise.resolve(result);
            }
            if (request.address === undefined || request.address === null || request.address < 0) {
                result.error = "Invalid Modbus Address";
                return Promise.resolve(result);
            }
            if (!request.quantity || request.quantity < 1) {
                result.error = "Invalid Modbus Quantity";
                return Promise.resolve(result);
            }
            if (request.unitid === undefined || request.unitid === null) {
                result.error = "Invalid Modbus Unit ID";
                return Promise.resolve(result);
            }

            return new Promise(async (resolve) => {
                let conn;
                try {
                    conn = await getModbusConnection(ip, parseInt(port));
                } catch (err) {
                    result.error = `Connection error: ${err.message}`;
                    return resolve(result);
                }

                const startTime = new Date();
                const responseCallBack = (err, res) => {
                    releaseConnection(ip, parseInt(port));

                    if (!err) {
                        const endTime = new Date();
                        const duration = endTime - startTime;
                        result.status = "success";
                        result.buffer = Buffer.concat(res.response.data);
                        result.durationMs = duration;
                        resolve(result);
                    } else {
                        result.error = `Modbus error: ${err.message}`;
                        resolve(result);
                    }
                };

                try {
                    if (request.functioncode == 1) {
                        conn.readCoils({
                            address: request.address,
                            quantity: request.quantity,
                            extra: { unitId: request.unitid }
                        }, responseCallBack);
                    } else if (request.functioncode == 2) {
                        conn.readDiscreteInputs({
                            address: request.address,
                            quantity: request.quantity,
                            extra: { unitId: request.unitid }
                        }, responseCallBack);
                    } else if (request.functioncode == 3) {
                        conn.readHoldingRegisters({
                            address: request.address,
                            quantity: request.quantity,
                            extra: { unitId: request.unitid }
                        }, responseCallBack);
                    } else if (request.functioncode == 4) {
                        conn.readInputRegisters({
                            address: request.address,
                            quantity: request.quantity,
                            extra: { unitId: request.unitid }
                        }, responseCallBack);
                    } else {
                        releaseConnection(ip, parseInt(port));
                        result.error = `Unsupported function code: ${request.functioncode}`;
                        resolve(result);
                    }
                } catch (err) {
                    releaseConnection(ip, parseInt(port));
                    result.error = `Execution error: ${err.message}`;
                    resolve(result);
                }
            });
        };

        node.on('input', async function (msg, send, done) {
            // Support both Node-RED <1.0 and >=1.0
            send = send || function() { node.send.apply(node, arguments); };
            done = done || function(err) { if (err && node.logerror) node.error(err, msg); };

            if (Array.isArray(msg.payload)) {
                try {
                    const promises = msg.payload.map(req => executeQuery(req, node.ip, node.port));
                    const results = await Promise.all(promises);
                    msg.payload = results;

                    const successCount = results.filter(r => r.status === "success").length;
                    const failCount = results.length - successCount;
                    if (failCount === 0) {
                        node.status({ fill: "green", shape: "dot", text: `Success: ${successCount}/${results.length}` });
                    } else {
                        node.status({ fill: "yellow", shape: "dot", text: `Success: ${successCount}, Fail: ${failCount}` });
                    }

                    send(msg);
                    done();
                } catch (err) {
                    node.status({ fill: "red", shape: "dot", text: `Execution error: ${err.message}` });
                    done(err);
                }
            } else if (msg.payload && typeof msg.payload === 'object') {
                try {
                    const result = await executeQuery(msg.payload, node.ip, node.port);
                    msg.payload = result;

                    if (result.status === "success") {
                        node.status({ fill: "green", shape: "dot", text: `Response Received ${result.durationMs} ms` });
                        msg.responseBuffer = { buffer: result.buffer };
                        msg.ip = result.ip;
                        msg.port = result.port;
                    } else {
                        node.status({ fill: "red", shape: "dot", text: result.error });
                        if (node.logerror) {
                            node.error(result.error, msg);
                        }
                    }
                    send(msg);
                    done();
                } catch (err) {
                    node.status({ fill: "red", shape: "dot", text: err.message });
                    done(err);
                }
            } else {
                const errMsg = "msg.payload must be an object or an array of objects";
                node.status({ fill: "red", shape: "dot", text: errMsg });
                done(new Error(errMsg));
            }
        });

        node.on('close', function(removed, done) {
            if (typeof removed === 'function') {
                done = removed;
            }
            for (let [key, connObj] of node.connectionPool.entries()) {
                if (connObj.connection) {
                    try {
                        connObj.connection.close(() => {});
                    } catch(e) {}
                }
                if (connObj.idleTimer) clearTimeout(connObj.idleTimer);
            }
            node.connectionPool.clear();
            if (done) done();
        });
    }
    RED.nodes.registerType("modbus-tcp-ip", ModbusTcpIpNode);
};