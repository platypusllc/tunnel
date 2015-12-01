package com.platypus.crw.udp;

// TODO: finish this class!

import com.platypus.crw.*;
import com.platypus.crw.VehicleServer.CameraState;
import com.platypus.crw.VehicleServer.WaypointState;
import com.platypus.crw.data.SensorData;
import com.platypus.crw.data.Twist;
import com.platypus.crw.data.UtmPose;
import com.platypus.crw.udp.UdpServer.Request;
import com.platypus.crw.udp.UdpServer.Response;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A service that registers a vehicle server over UDP to allow control over the
 * network via a proxy server.
 *
 * @author Prasanna Velagapudi <psigen@gmail.com>
 */
@SuppressWarnings("LoggerStringConcat")
public class UdpVehicleService2 implements UdpServer.RequestHandler {
    private static final Logger logger = Logger.getLogger(UdpVehicleService2.class.getName());
    private static final SocketAddress DUMMY_ADDRESS = new InetSocketAddress(0);

    protected AsyncVehicleServer _vehicleServer;
    protected final Object _serverLock = new Object();
    protected final AtomicInteger _imageSeq = new AtomicInteger();

    // Start ticket with random offset to prevent collisions across multiple clients
    protected final AtomicLong _ticketCounter = new AtomicLong(new Random().nextLong() << 32);

    protected final UdpServer _udpServer;

    protected final List<SocketAddress> _registries = new ArrayList<>();
    protected final Map<SocketAddress, Integer> _poseListeners = new LinkedHashMap<>();
    protected final Map<SocketAddress, Integer> _imageListeners = new LinkedHashMap<>();
    protected final Map<SocketAddress, Integer> _cameraListeners = new LinkedHashMap<>();
    protected final Map<Integer, Map<SocketAddress,Integer>> _sensorListeners = new TreeMap<>();
    protected final Map<SocketAddress, Integer> _velocityListeners = new LinkedHashMap<>();
    protected final Map<SocketAddress, Integer> _waypointListeners = new LinkedHashMap<>();
    protected final Timer _registrationTimer = new Timer();

    public UdpVehicleService2(int port) {
        _udpServer = (port > 0) ? new UdpServer(port) : new UdpServer();
        _udpServer.setHandler(this);
        _udpServer.start();

        _registrationTimer.scheduleAtFixedRate(_registrationTask, 0, UdpConstants.REGISTRATION_RATE_MS);
    }

    public UdpVehicleService2() {
        this(-1);
    }

    public UdpVehicleService2(VehicleServer server) {
        this(AsyncVehicleServer.Util.toAsync(server));
    }

    public UdpVehicleService2(int port, VehicleServer server) {
        this(port, AsyncVehicleServer.Util.toAsync(server));
    }

    public UdpVehicleService2(AsyncVehicleServer server) {
        this();
        setServer(server);
    }

    public UdpVehicleService2(int port, AsyncVehicleServer server) {
        this(port);
        setServer(server);
    }

    public SocketAddress getSocketAddress() {
        return _udpServer.getSocketAddress();
    }

    public final void setServer(AsyncVehicleServer server) {
        synchronized(_serverLock) {
            if (_vehicleServer != null) unregister();
            _vehicleServer = server;
            if (_vehicleServer != null) register();
        }
    }

    private void register() {
        synchronized (_cameraListeners) {
            if (!_cameraListeners.isEmpty()) {
                _vehicleServer.addCameraListener(_handler, null);
            }
        }

        synchronized (_imageListeners) {
            if (!_imageListeners.isEmpty()) {
                _vehicleServer.addImageListener(_handler, null);
            }
        }

        synchronized (_poseListeners) {
            if (!_poseListeners.isEmpty()) {
                _vehicleServer.addPoseListener(_handler, null);
            }
        }

        synchronized (_velocityListeners) {
            if (!_velocityListeners.isEmpty()) {
                _vehicleServer.addVelocityListener(_handler, null);
            }
        }

        synchronized (_waypointListeners) {
            if (!_waypointListeners.isEmpty()) {
                _vehicleServer.addWaypointListener(_handler, null);
            }
        }

        synchronized (_sensorListeners) {
            for (Map.Entry<Integer, Map<SocketAddress, Integer>> entry : _sensorListeners.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    _vehicleServer.addSensorListener(entry.getKey(), _handler, null);
                }
            }
        }
    }

    private void unregister() {
        _vehicleServer.removeCameraListener(_handler, null);
        _vehicleServer.removeImageListener(_handler, null);
        _vehicleServer.removePoseListener(_handler, null);
        _vehicleServer.removeVelocityListener(_handler, null);
        _vehicleServer.removeWaypointListener(_handler, null);

        _vehicleServer.getNumSensors(new FunctionObserver<Integer>() {
            @Override
            public void completed(Integer i) {
                _vehicleServer.removeSensorListener(i, _handler, null);
            }

            @Override
            public void failed(FunctionError functionError) {
                logger.warning("Failed to get number of sensors.");
            }
        });
    }

    public final AsyncVehicleServer getServer() {
        synchronized(_serverLock) {
            return _vehicleServer;
        }
    }

    public void addRegistry(InetSocketAddress addr) {
        synchronized(_registries) {
            _registries.add(addr);
        }
    }

    public void removeRegistry(InetSocketAddress addr) {
        synchronized(_registries) {
            _registries.remove(addr);
        }
    }

    public SocketAddress[] listRegistries() {
        synchronized(_registries) {
            return _registries.toArray(new SocketAddress[0]);
        }
    }

    @Override
    public void received(Request req) {

        try {
            final String command = req.stream.readUTF();
            final Response resp = new Response(req);
            resp.stream.writeUTF(command);

            // TODO: remove me
            //logger.log(Level.INFO, "Received command " + req.ticket + ": " + command + ", " + UdpConstants.COMMAND.fromStr(command));

            switch (UdpConstants.COMMAND.fromStr(command)) {
                case CMD_REGISTER_POSE_LISTENER: {
                    synchronized (_poseListeners) {
                        if (_poseListeners.isEmpty())
                            _vehicleServer.addPoseListener(_handler, null);
                        _poseListeners.put(req.source, UdpConstants.REGISTRATION_TIMEOUT_COUNT);
                    }
                    break;
                }
                case CMD_SET_POSE: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to set pose: " + functionError);
                        }
                    };

                    _vehicleServer.setPose(UdpConstants.readPose(req.stream), obs);
                    break;
                }
                case CMD_GET_POSE: {
                    FunctionObserver<UtmPose> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<UtmPose>() {

                        @Override
                        public void completed(UtmPose pose) {
                            try {
                                UdpConstants.writePose(resp.stream, pose);
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write pose.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get pose: " + functionError);
                        }
                    };

                    _vehicleServer.getPose(obs);
                    break;
                }
                case CMD_REGISTER_IMAGE_LISTENER: {
                    synchronized (_imageListeners) {
                        if (_imageListeners.isEmpty())
                            _vehicleServer.addImageListener(_handler, null);
                        _imageListeners.put(req.source, UdpConstants.REGISTRATION_TIMEOUT_COUNT);
                    }
                    break;
                }
                case CMD_CAPTURE_IMAGE: {
                    FunctionObserver<byte[]> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<byte[]>() {

                        @Override
                        public void completed(byte[] image) {
                            try {
                                resp.stream.writeInt(image.length);
                                resp.stream.write(image);
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write captured image.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get captured image: " + functionError);
                        }
                    };

                    _vehicleServer.captureImage(req.stream.readInt(), req.stream.readInt(), obs);
                    break;
                }
                case CMD_REGISTER_CAMERA_LISTENER: {
                    synchronized (_cameraListeners) {
                        if (_cameraListeners.isEmpty())
                            _vehicleServer.addCameraListener(_handler, null);
                        _cameraListeners.put(req.source, UdpConstants.REGISTRATION_TIMEOUT_COUNT);
                    }
                    break;
                }
                case CMD_START_CAMERA: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to start camera: " + functionError);
                        }
                    };

                    _vehicleServer.startCamera(
                            req.stream.readInt(), req.stream.readDouble(),
                            req.stream.readInt(), req.stream.readInt(), obs);
                    break;
                }
                case CMD_STOP_CAMERA: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to stop camera: " + functionError);
                        }
                    };

                    _vehicleServer.stopCamera(obs);
                    break;
                }
                case CMD_GET_CAMERA_STATUS: {
                    FunctionObserver<CameraState> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<CameraState>() {

                        @Override
                        public void completed(CameraState state) {
                            try {
                                resp.stream.writeByte(state.ordinal());
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write camera status.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get camera status: " + functionError);
                        }
                    };

                    _vehicleServer.getCameraStatus(obs);
                    break;
                }
                case CMD_REGISTER_SENSOR_LISTENER:
                    synchronized(_sensorListeners) {
                        int channel = req.stream.readInt();

                        // Retrieve the sensor sublisteners
                        Map<SocketAddress, Integer> _listeners = _sensorListeners.get(channel);
                        if (_listeners == null) {
                            _listeners = new LinkedHashMap<>();
                            _sensorListeners.put(channel, _listeners);
                        }

                        // Add the address to the appropriate sublistener list
                        if (_listeners.isEmpty())
                            _vehicleServer.addSensorListener(channel, _handler, null);
                        _listeners.put(req.source, UdpConstants.REGISTRATION_TIMEOUT_COUNT);
                    }
                    break;
                case CMD_SET_SENSOR_TYPE: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to set sensor type: " + functionError);
                        }
                    };

                    _vehicleServer.setSensorType(req.stream.readInt(),
                            VehicleServer.SensorType.values()[req.stream.readByte()], obs);
                    break;
                }
                case CMD_GET_SENSOR_TYPE: {
                    FunctionObserver<VehicleServer.SensorType> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<VehicleServer.SensorType>() {

                        @Override
                        public void completed(VehicleServer.SensorType type) {
                            try {
                                resp.stream.writeByte(type.ordinal());
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write sensor type.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get sensor type: " + functionError);
                        }
                    };

                    _vehicleServer.getSensorType(req.stream.readInt(), obs);
                    break;
                }
                case CMD_GET_NUM_SENSORS: {
                    FunctionObserver<Integer> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Integer>() {

                        @Override
                        public void completed(Integer numSensors) {
                            try {
                                resp.stream.writeInt(numSensors);
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write number of sensors.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get number of sensors: " + functionError);
                        }
                    };

                    _vehicleServer.getNumSensors(obs);
                    break;
                }
                case CMD_REGISTER_VELOCITY_LISTENER: {
                    synchronized (_velocityListeners) {
                        if (_velocityListeners.isEmpty())
                            _vehicleServer.addVelocityListener(_handler, null);
                        _velocityListeners.put(req.source, UdpConstants.REGISTRATION_TIMEOUT_COUNT);
                    }
                    break;
                }
                case CMD_SET_VELOCITY: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to set velocity: " + functionError);
                        }
                    };

                    _vehicleServer.setVelocity(UdpConstants.readTwist(req.stream), obs);
                    break;
                }
                case CMD_GET_VELOCITY: {
                    FunctionObserver<Twist> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Twist>() {

                        @Override
                        public void completed(Twist twist) {
                            try {
                                UdpConstants.writeTwist(resp.stream, twist);
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write velocity.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get velocity: " + functionError);
                        }
                    };

                    _vehicleServer.getVelocity(obs);
                    _udpServer.respond(resp);
                    break;
                }
                case CMD_REGISTER_WAYPOINT_LISTENER: {
                    synchronized (_waypointListeners) {
                        if (_waypointListeners.isEmpty())
                            _vehicleServer.addWaypointListener(_handler, null);
                        _waypointListeners.put(req.source, UdpConstants.REGISTRATION_TIMEOUT_COUNT);
                    }
                    break;
                }
                case CMD_START_WAYPOINTS: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to start waypoints: " + functionError);
                        }
                    };

                    UtmPose[] swPoses = new UtmPose[req.stream.readInt()];
                    for (int i = 0; i < swPoses.length; ++i) {
                        swPoses[i] = UdpConstants.readPose(req.stream);
                    }

                    _vehicleServer.startWaypoints(swPoses, req.stream.readUTF(), obs);
                    break;
                }
                case CMD_STOP_WAYPOINTS: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to stop waypoints: " + functionError);
                        }
                    };

                    _vehicleServer.stopWaypoints(obs);
                    break;
                }
                case CMD_GET_WAYPOINTS: {
                    FunctionObserver<UtmPose[]> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<UtmPose[]>() {

                        @Override
                        public void completed(UtmPose[] poses) {
                            try {
                                resp.stream.writeInt(poses.length);
                                for (UtmPose pose : poses) {
                                    UdpConstants.writePose(resp.stream, pose);
                                }
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write waypoints.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get waypoints: " + functionError);
                        }
                    };

                    _vehicleServer.getWaypoints(obs);
                    break;
                }
                case CMD_GET_WAYPOINT_STATUS: {
                    FunctionObserver<WaypointState> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<WaypointState>() {

                        @Override
                        public void completed(WaypointState state) {
                            try {
                                resp.stream.writeByte(state.ordinal());
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write waypoint status.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get waypoint status: " + functionError);
                        }
                    };

                    _vehicleServer.getWaypointStatus(obs);
                    break;
                }
                case CMD_IS_CONNECTED: {
                    if (_vehicleServer == null) {
                        if (resp.ticket != UdpConstants.NO_TICKET) {
                            resp.stream.writeBoolean(false);
                            _udpServer.respond(resp);
                        }
                        break;
                    } else {
                        FunctionObserver<Boolean> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                                null : new FunctionObserver<Boolean>() {

                            @Override
                            public void completed(Boolean isConnected) {
                                try {
                                    resp.stream.writeBoolean(isConnected);
                                    _udpServer.respond(resp);
                                } catch (IOException e) {
                                    logger.warning("Failed to write connected status.");
                                }
                            }

                            @Override
                            public void failed(FunctionError functionError) {
                                logger.warning("Failed to get connected status: " + functionError);
                            }
                        };

                        _vehicleServer.isConnected(obs);
                        break;
                    }
                }
                case CMD_IS_AUTONOMOUS: {
                    FunctionObserver<Boolean> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Boolean>() {

                        @Override
                        public void completed(Boolean isAutonomous) {
                            try {
                                resp.stream.writeBoolean(isAutonomous);
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write autonomous status.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get autonomous status: " + functionError);
                        }
                    };

                    _vehicleServer.isAutonomous(obs);
                    break;
                }
                case CMD_SET_AUTONOMOUS: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to set autonomous: " + functionError);
                        }
                    };

                    _vehicleServer.setAutonomous(req.stream.readBoolean(), obs);
                    break;
                }
                case CMD_SET_GAINS: {
                    FunctionObserver<Void> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<Void>() {

                        @Override
                        public void completed(Void aVoid) {
                            _udpServer.respond(resp);
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to set gains: " + functionError);
                        }
                    };

                    int sgAxis = req.stream.readInt();
                    double[] sgGains = new double[req.stream.readInt()];
                    for (int i = 0; i < sgGains.length; ++i) {
                        sgGains[i] = req.stream.readDouble();
                    }
                    _vehicleServer.setGains(sgAxis, sgGains, obs);
                    break;
                }
                case CMD_GET_GAINS: {
                    FunctionObserver<double[]> obs = (resp.ticket == UdpConstants.NO_TICKET) ?
                            null : new FunctionObserver<double[]>() {

                        @Override
                        public void completed(double[] gains) {
                            try {
                                resp.stream.writeInt(gains.length);
                                for (double gain : gains) {
                                    resp.stream.writeDouble(gain);
                                }
                                _udpServer.respond(resp);
                            } catch (IOException e) {
                                logger.warning("Failed to write gains.");
                            }
                        }

                        @Override
                        public void failed(FunctionError functionError) {
                            logger.warning("Failed to get gains: " + functionError);
                        }
                    };

                    _vehicleServer.getGains(req.stream.readInt(), obs);
                    break;
                }
                case CMD_CONNECT: {
                    // Unpack the forwarded server
                    String hostname = req.stream.readUTF();
                    int port = req.stream.readInt();
                    SocketAddress addr = new InetSocketAddress(hostname, port);

                    // Send off a one-time command to the forwarded server
                    if (resp.ticket != UdpConstants.NO_TICKET) {
                        Response r = new Response(req.ticket, addr);
                        r.stream.writeUTF(UdpConstants.COMMAND.CMD_CONNECT.str);
                        _udpServer.respond(r);
                    }
                    break;
                }
                default:
                    String warning = "Ignoring unknown command: " + command;
                    logger.log(Level.WARNING, warning);
            }
        } catch (IOException e) {
            String warning = "Failed to parse request: " + req.ticket;
            logger.log(Level.WARNING, warning);
        }

    }

    @Override
    public void timeout(long ticket, SocketAddress destination) {
        String warning = "No response for: " + ticket + " @ " + destination;
        logger.warning(warning);
    }

    /**
     * Terminates service processes and de-registers the service from a
     * registry, if one was being used.
     */
    public void shutdown() {
        _udpServer.stop();
        _registrationTimer.cancel();
        _registrationTimer.purge();
    }

    // TODO: Clean up old streams!!
    private final StreamHandler _handler = new StreamHandler();

    private class StreamHandler implements PoseListener, ImageListener, CameraListener, SensorListener, VelocityListener, WaypointListener {

        @Override
        public void receivedPose(UtmPose pose) {
            // Quickly check if anyone is listening
            synchronized(_poseListeners) {
                if (_poseListeners.isEmpty()) return;
            }

            try {
                // Construct message
                Response resp = new Response(UdpConstants.NO_TICKET, DUMMY_ADDRESS);
                resp.stream.writeUTF(UdpConstants.COMMAND.CMD_SEND_POSE.str);
                UdpConstants.writePose(resp.stream, pose);

                // Send to all listeners
                synchronized(_poseListeners) {
                    _udpServer.bcast(resp, _poseListeners.keySet());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize pose");
            }
        }

        @Override
        public void receivedImage(byte[] image) {
            // Quickly check if anyone is listening
            synchronized(_imageListeners) {
                if (_imageListeners.isEmpty()) return;
            }

            try {
                final int imageSeq = _imageSeq.incrementAndGet();

                // Figure out how many pieces into which to fragment the image
                final int totalIdx = (image.length - 1) / UdpConstants.MAX_PAYLOAD_SIZE + 1;

                // Transmit each piece in a separate packet
                for (int pieceIdx = 0; pieceIdx < totalIdx; ++pieceIdx) {

                    // Compute the length of this piece
                    int pieceLen = (pieceIdx + 1 < totalIdx) ? UdpConstants.MAX_PAYLOAD_SIZE : image.length - pieceIdx*UdpConstants.MAX_PAYLOAD_SIZE;

                    // Send to all listeners
                    synchronized(_imageListeners) {
                        for (SocketAddress il : _imageListeners.keySet()) {
                            // Construct message
                            // TODO: find a more efficient way to handle this serialization
                            Response resp = new Response(_ticketCounter.incrementAndGet(), il);
                            resp.stream.writeUTF(UdpConstants.COMMAND.CMD_SEND_IMAGE.str);
                            resp.stream.writeInt(imageSeq);
                            resp.stream.writeInt(totalIdx);
                            resp.stream.writeInt(pieceIdx);

                            resp.stream.writeInt(pieceLen);
                            resp.stream.write(image, pieceIdx*UdpConstants.MAX_PAYLOAD_SIZE, pieceLen);

                            _udpServer.respond(resp);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize image");
            }
        }

        @Override
        public void imagingUpdate(CameraState status) {
            // Quickly check if anyone is listening
            synchronized(_cameraListeners) {
                if (_cameraListeners.isEmpty()) return;
            }

            try {
                // Construct message
                Response resp = new Response(UdpConstants.NO_TICKET, DUMMY_ADDRESS);
                resp.stream.writeUTF(UdpConstants.COMMAND.CMD_SEND_CAMERA.str);
                resp.stream.writeByte(status.ordinal());

                // Send to all listeners
                synchronized(_cameraListeners) {
                    _udpServer.bcast(resp, _cameraListeners.keySet());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize camera");
            }
        }

        @Override
        public void receivedSensor(SensorData sensor) {
            // Quickly check if anyone is listening
            synchronized(_sensorListeners) {
                if (!_sensorListeners.containsKey(sensor.channel)) return;
            }

            try {
                // Construct message
                Response resp = new Response(UdpConstants.NO_TICKET, DUMMY_ADDRESS);
                resp.stream.writeUTF(UdpConstants.COMMAND.CMD_SEND_SENSOR.str);
                UdpConstants.writeSensorData(resp.stream, sensor);

                // Send to all listeners
                synchronized(_sensorListeners) {
                    Map<SocketAddress, Integer> _listeners = _sensorListeners.get(sensor.channel);
                    _udpServer.bcast(resp, _listeners.keySet());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize sensor " + sensor.channel);
            }
        }

        @Override
        public void receivedVelocity(Twist velocity) {
            // Quickly check if anyone is listening
            synchronized(_velocityListeners) {
                if (_velocityListeners.isEmpty()) return;
            }

            try {
                // Construct message
                Response resp = new Response(UdpConstants.NO_TICKET, DUMMY_ADDRESS);
                resp.stream.writeUTF(UdpConstants.COMMAND.CMD_SEND_VELOCITY.str);
                UdpConstants.writeTwist(resp.stream, velocity);

                // Send to all listeners
                synchronized(_velocityListeners) {
                    _udpServer.bcast(resp, _velocityListeners.keySet());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize camera");
            }
        }

        @Override
        public void waypointUpdate(WaypointState status) {
            // Quickly check if anyone is listening
            synchronized(_waypointListeners) {
                if (_waypointListeners.isEmpty()) return;
            }

            try {
                // Construct message
                Response resp = new Response(UdpConstants.NO_TICKET, DUMMY_ADDRESS);
                resp.stream.writeUTF(UdpConstants.COMMAND.CMD_SEND_WAYPOINT.str);
                resp.stream.writeByte(status.ordinal());

                // Send to all listeners
                synchronized(_waypointListeners) {
                    _udpServer.bcast(resp, _waypointListeners.keySet());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize camera");
            }
        }
    }

    /**
     * Takes a list of registered listeners and their timeout counts, and
     * decrements the timeouts.  If a listener count hits zero, it is removed.
     *
     * @param registrationList the list that should be updated
     */
    protected void updateRegistrations(final Map<SocketAddress, Integer> registrationList) {
        synchronized(registrationList) {
            for (Iterator<Map.Entry<SocketAddress, Integer>> it = registrationList.entrySet().iterator(); it.hasNext();) {
                Map.Entry<SocketAddress, Integer> e = it.next();
                if (e.getValue() == 0) {
                    it.remove();
                } else {
                    e.setValue(e.getValue() - 1);
                }
            }
        }
    }

    protected TimerTask _registrationTask = new TimerTask() {
        final Response resp = new Response(UdpConstants.NO_TICKET, DUMMY_ADDRESS);
        {
            try {
                resp.stream.writeUTF(UdpConstants.COMMAND.CMD_REGISTER.str);
                resp.stream.writeUTF("Vehicle");
            } catch (IOException e) {
                throw new RuntimeException("Failed to construct registration message.", e);
            }
        }

        @Override
        public void run() {
            // Send registration commands to all the specified registries
            synchronized(_registries) {
                _udpServer.bcast(resp, _registries);
            }

            // Update each of the registration lists to remove outdated listeners
            synchronized (_poseListeners) {
                updateRegistrations(_poseListeners);
                if (_poseListeners.isEmpty())
                    _vehicleServer.removePoseListener(_handler, null);
            }

            synchronized (_imageListeners) {
                updateRegistrations(_imageListeners);
                if (_imageListeners.isEmpty())
                    _vehicleServer.removeImageListener(_handler, null);
            }

            synchronized (_cameraListeners) {
                updateRegistrations(_cameraListeners);
                if (_cameraListeners.isEmpty())
                    _vehicleServer.removeCameraListener(_handler, null);
            }

            synchronized (_velocityListeners) {
                updateRegistrations(_velocityListeners);
                if (_velocityListeners.isEmpty())
                    _vehicleServer.removeVelocityListener(_handler, null);
            }

            synchronized (_waypointListeners) {
                updateRegistrations(_waypointListeners);
                if (_waypointListeners.isEmpty())
                    _vehicleServer.removeWaypointListener(_handler, null);
            }

            synchronized(_sensorListeners) {
                for (Map.Entry<Integer, Map<SocketAddress, Integer>> sensorListener : _sensorListeners.entrySet()) {
                    updateRegistrations(sensorListener.getValue());
                    if (sensorListener.getValue().isEmpty())
                        _vehicleServer.removeSensorListener(sensorListener.getKey(), _handler, null);
                }
            }
        }
    };
}