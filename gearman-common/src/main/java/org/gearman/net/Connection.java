package org.gearman.net;

import com.google.common.primitives.Ints;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.PacketFactory;

import org.gearman.common.packets.request.EchoRequest;
import org.gearman.common.packets.response.EchoResponse;
import org.gearman.constants.GearmanConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;


public class Connection {
    protected Socket socket;
    protected String hostname;
    protected int port;
    private Logger LOG = LoggerFactory.getLogger(Connection.class);
    private Long lastTimeSeenAlive;

    public Connection()
    {	}


    public Connection(String hostname, int port)
    {
        this.hostname = hostname;
        this.port = port;
    }

    public Connection(Socket socket)
    {
        this.socket = socket;
    }


    public void sendPacket(Packet p) throws IOException
    {
        initializeConnection();
        socket.getOutputStream().write(p.toByteArray());
    }

    public void close() throws IOException
    {
        socket.close();
    }

    public String toString()
    {
        return String.format("%s:%d", this.hostname, this.port);
    }


    public boolean isHealthy()
    {
        try {
            initializeConnection();

            this.sendPacket(new EchoRequest("OK"));
            EchoResponse response = (EchoResponse)(this.getNextPacket());

            if(response != null)
            {
                byte[] data = response.getData();
                byte[] matchData = "OK".getBytes(GearmanConstants.CHARSET);
                if(Arrays.equals(data, matchData))
                {
                    this.updateLastTimeSeenAlive();
                    return true;
                }
            }

        } catch (IOException ioe) {
            LOG.error("Client unable to write to socket: " + ioe.toString());
            try {
                this.socket.close();
            } catch (IOException closeException) {
                LOG.error("Unable to close dead socket: " + closeException.toString());
            }
        }

        return false;
    }

    public Packet getNextPacket() throws IOException
    {
        initializeConnection();

        int messagesize = -1;

        // Initialize to 12 bytes (header only), and resize later as needed
        byte[] header = new byte[12];
        byte[] packetBytes;

        InputStream is = socket.getInputStream();
        try {

            int numbytes = is.read(header, 0, 12);

            if(numbytes == 12)
            {
                // Check byte count
                byte[] sizebytes = Arrays.copyOfRange(header, 8, 12);

                messagesize = Ints.fromByteArray(sizebytes);

                if (messagesize > 0)
                {
                    // Grow packet buffer to fit data
                    packetBytes = Arrays.copyOf(header, 12 + messagesize);
                } else {
                    packetBytes = header;
                }

                is.read(packetBytes, 12, messagesize);

                return PacketFactory.packetFromBytes(packetBytes);

            } else if(numbytes == -1) {

            }
        } catch (Exception e) {
            LOG.error("Exception reading data: ", e);
            e.printStackTrace();
            return null;
        }

        return null;
    }

    public Long getLastTimeSeenAlive() {
        return lastTimeSeenAlive;
    }

    public void setLastTimeSeenAlive(Long lastTimeSeenAlive) {
        this.lastTimeSeenAlive = lastTimeSeenAlive;
    }

    private void initializeConnection() throws IOException
    {
        if(socket == null ||
           socket.isClosed())
        {
            socket = new Socket(hostname, port);
        }
    }

    public void updateLastTimeSeenAlive() {
        this.lastTimeSeenAlive = new Date().getTime();
    }
}



