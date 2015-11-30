package com.platypus.crw.udp;

import com.platypus.crw.AsyncVehicleServer;
import com.platypus.crw.udp.UdpServer.Request;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standalone tunnel for forwarding vehicle data between two firewalled clients.
 * Clients that use this address for a registry will receive the address of a proxy
 * hosted on this machine.  The proxy will forward requests to the true vehicle server.
 *
 * This class should be run standalone on a publicly visible server with accessible UDP ports.
 * This is necessary for tunnels to be established from the vehicle server and client.
 *
 * @author Prasanna Velagapudi <psigen@gmail.com>
 */
public class VehicleTunnelService {
    private static final Logger logger = Logger.getLogger(VehicleTunnelService.class.getName());

    public static final int DEFAULT_UDP_PORT = 6077;

    protected final String _hostname;
    protected final UdpServer _udpServer;
    protected final Timer _registrationTimer = new Timer();
    protected final Map<SocketAddress, Client> _clients = new LinkedHashMap<>();

    protected static class Client {
        int ttl;
        String name;
        UdpVehicleProxy vehicle;
        UdpVehicleService tunnel;
        InetSocketAddress tunnelAddr;
    }

    /**
     * A thin wrapper class to expose the local socket address used for this UdpVehicleServer.
     */
    protected static class UdpVehicleProxy extends UdpVehicleServer {
        public InetSocketAddress getLocalSocketAddress() {
            return (InetSocketAddress)this._udpServer.getSocketAddress();
        }
    }

    /**
     * Default constructor that starts a server using the default localhost address and port (6077).
     *
     * This constructor is NOT recommended, as the address on which the server is accessible may not
     * be introspectable, in which case the server will broadcast the wrong connection address.
     *
     * It is better to explicitly specify the hostname that the server should send to clients.
     *
     * @throws UnknownHostException
     */
    public VehicleTunnelService() throws UnknownHostException {
        this(InetAddress.getLocalHost().getHostAddress(), DEFAULT_UDP_PORT);
    }

    /**
     * Starts the server using the specified hostname and port.
     *
     * The provided hostname will be used by clients to create connections back to this server, so a
     * publicly-reachable hostname or address is required.
     *
     * @param hostname
     * @param udpPort
     */
    public VehicleTunnelService(String hostname, int udpPort) {
        _hostname = hostname;
        _udpServer = new UdpServer(udpPort);
        _udpServer.setHandler(_handler);
        _udpServer.start();

        _registrationTimer.scheduleAtFixedRate(_registrationTask, 0, UdpConstants.REGISTRATION_RATE_MS);
    }

    /**
     * Shuts down this server.  It cannot be restarted after this.
     */
    public void shutdown() {
        _udpServer.stop();

        synchronized(_clients) {
            for (Client client : _clients.values()) {
                client.tunnel.shutdown();
                client.vehicle.shutdown();
            }
        }
    }

    private final UdpServer.RequestHandler _handler = new UdpServer.RequestHandler() {

        @Override
        public void received(Request req) {
            try {
                final String command = req.stream.readUTF();
                switch (UdpConstants.COMMAND.fromStr(command)) {
                    case CMD_REGISTER:

                        synchronized(_clients) {
                            // Look for client in table
                            Client c = _clients.get(req.source);

                            // If not found, create a new entry
                            if (c == null) {
                                c = new Client();
                                c.vehicle = new UdpVehicleProxy();
                                c.tunnel = new UdpVehicleService(AsyncVehicleServer.Util.toSync(c.vehicle));
                                c.tunnelAddr = new InetSocketAddress(_hostname,
                                        ((InetSocketAddress)c.tunnel.getSocketAddress()).getPort());
                                _clients.put(req.source, c);
                            }

                            // Update the registration properties for this client.
                            c.vehicle.setVehicleService(req.source);
                            c.name = req.stream.readUTF();
                            c.ttl = UdpConstants.REGISTRATION_TIMEOUT_COUNT;
                        }
                        break;
                    case CMD_CONNECT:

                        // Unpack address to which to connect.
                        String hostname = req.stream.readUTF();
                        int port = req.stream.readInt();
                        InetSocketAddress addr = new InetSocketAddress(hostname, port);

                        // Find a client that matches the incoming connection request.
                        Client c = null;
                        synchronized(_clients) {
                            for (Map.Entry<SocketAddress, Client> e : _clients.entrySet()) {
                                if (e.getValue().tunnelAddr.equals(addr)) {
                                    c = e.getValue();
                                    break;
                                }
                            }
                        }

                        // Ignore requests for non-tunneled clients.
                        if (c == null) {
                            logger.warning("Connection requested for invalid client: " + addr);
                            return;
                        }

                        logger.info("Connection request for: " +
                                req.source + " -> " +
                                c.tunnelAddr.getPort() + " -> " +
                                c.vehicle.getVehicleService());

                        // Forward this connection request to the server in question.
                        synchronized(c) {
                            UdpServer.Response respCon = new UdpServer.Response(req.ticket, c.vehicle.getVehicleService());
                            respCon.stream.writeUTF(command);
                            respCon.stream.writeUTF(_hostname);
                            respCon.stream.writeInt(c.vehicle.getLocalSocketAddress().getPort());
                            _udpServer.respond(respCon);
                        }
                        break;
                    case CMD_LIST:

                        // Create a response to the same client
                        UdpServer.Response respList = new UdpServer.Response(req);
                        respList.stream.writeUTF(command);

                        // List all of the clients
                        synchronized(_clients) {
                            respList.stream.writeInt(_clients.size());
                            for (Map.Entry<SocketAddress, Client> e : _clients.entrySet()) {
                                respList.stream.writeUTF(e.getValue().name);
                                respList.stream.writeUTF(_hostname);
                                respList.stream.writeInt(e.getValue().tunnelAddr.getPort());
                            }
                        }
                        _udpServer.respond(respList);
                        break;
                    case CMD_REGISTER_POSE_LISTENER:
                    case CMD_SEND_POSE:
                    case CMD_SET_POSE:
                    case CMD_GET_POSE:
                    case CMD_REGISTER_IMAGE_LISTENER:
                    case CMD_SEND_IMAGE:
                    case CMD_CAPTURE_IMAGE:
                    case CMD_REGISTER_CAMERA_LISTENER:
                    case CMD_SEND_CAMERA:
                    case CMD_START_CAMERA:
                    case CMD_STOP_CAMERA:
                    case CMD_GET_CAMERA_STATUS:
                    case CMD_REGISTER_SENSOR_LISTENER:
                    case CMD_SEND_SENSOR:
                    case CMD_SET_SENSOR_TYPE:
                    case CMD_GET_SENSOR_TYPE:
                    case CMD_GET_NUM_SENSORS:
                    case CMD_REGISTER_VELOCITY_LISTENER:
                    case CMD_SEND_VELOCITY:
                    case CMD_SET_VELOCITY:
                    case CMD_GET_VELOCITY:
                    case CMD_REGISTER_WAYPOINT_LISTENER:
                    case CMD_SEND_WAYPOINT:
                    case CMD_START_WAYPOINTS:
                    case CMD_STOP_WAYPOINTS:
                    case CMD_GET_WAYPOINTS:
                    case CMD_GET_WAYPOINT_STATUS:
                    case CMD_IS_CONNECTED:
                    case CMD_IS_AUTONOMOUS:
                    case CMD_SET_AUTONOMOUS:
                    case CMD_SET_GAINS:
                    case CMD_GET_GAINS:
                    case UNKNOWN:
                        break;
                    default:
                        logger.log(Level.WARNING, "Ignoring unknown command: {0}", command);
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "Failed to parse request: {0}", req.ticket);
            }
        }

        @Override
        public void timeout(long ticket, SocketAddress destination) {
            logger.log(Level.SEVERE, "Unexpected timeout: {0}", ticket);
        }
    };

    // Removes outdated registrations from client list
    protected TimerTask _registrationTask = new TimerTask() {
        @Override
        public void run() {
        synchronized(_clients) {
            for (Iterator<Map.Entry<SocketAddress, Client>> it = _clients.entrySet().iterator(); it.hasNext();) {
                Map.Entry<SocketAddress, Client> client = it.next();
                if (client.getValue().ttl == 0) {
                    client.getValue().tunnel.shutdown();
                    client.getValue().vehicle.shutdown();
                    it.remove();
                } else {
                    client.getValue().ttl--;
                }
            }
        }
        }
    };

    /**
     * Returns a map containing the current set of registered vehicles.
     *
     * This list contains the true addresses of vehicles, <b>not their tunnel proxy addresses</b>.  It is intended for
     * debugging, and should not be used to retrieve addresses for client use.
     *
     * @return a map from the socket address of clients (vehicles) to their text names
     */
    public Map<SocketAddress, String> getClients() {
        HashMap<SocketAddress, String> map = new LinkedHashMap<>();

        synchronized(_clients) {
            for (Client client : _clients.values()) {
                map.put(client.vehicle.getVehicleService(), client.name);
            }
        }

        return map;
    }

    /**
     * Simple startup script that runs the VehicleTunnelService using the
     * specified UDP port if available and prints a list of connected clients.
     *
     * @param args takes a UDP port number as the first argument.
     */
    public static void main(String args[]) throws UnknownHostException {
        final String hostname = (args.length > 0) ? args[0] : InetAddress.getLocalHost().getHostAddress();
        final int port = (args.length > 1) ? Integer.parseInt(args[1]) : DEFAULT_UDP_PORT;

        final VehicleTunnelService service = new VehicleTunnelService(hostname, port);
        System.out.println("Started vehicle tunnel: " + hostname + ":" + port);

        // Periodically print the registered clients
        Timer printer = new Timer();
        printer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Collection<Map.Entry<SocketAddress, String>> clients = service.getClients().entrySet();
                if (clients.size() > 0) {
                    System.out.println("CLIENT LIST:");
                    for (Map.Entry<SocketAddress, String> e : clients) {
                        System.out.println("\t" + e.getValue() + " : " + e.getKey());
                    }
                } else {
                    System.out.println("NO CLIENTS.");
                }
            }
        }, 0, 1000);
    }
}