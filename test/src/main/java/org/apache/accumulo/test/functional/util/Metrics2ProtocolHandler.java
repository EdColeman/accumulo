package org.apache.accumulo.test.functional.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class Metrics2ProtocolHandler {

  private static final int BUFFER_SIZE = 64 * 1024;

  private static final Logger log = LoggerFactory.getLogger(Metrics2ProtocolHandler.class);

  private DataInputStream in = null;
  private DataOutputStream out = null;

  private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

  public Metrics2ProtocolHandler(final DataInputStream in, final DataOutputStream out) {
    requireNonNull(in);
    requireNonNull(out);
    this.in = in;
    this.out = out;
  }

  public synchronized String read() {

    try {

      log.debug("start blocking read");

      int length = in.readInt();

      if (length > BUFFER_SIZE) {
        throw new IllegalArgumentException(
            String.format("Message length %d exceeds buffer size %d", length, BUFFER_SIZE));
      }

      log.debug("Expecting {}", length);

      buffer.clear();

      in.readFully(buffer.array(), 0, length);

      buffer.flip().limit(length);

      return StandardCharsets.UTF_8.decode(buffer).toString();

    } catch (IOException ex) {
      throw new IllegalStateException("Send failed", ex);
    }

  }

  public synchronized void send(final byte[] payload) {
    try {

      int length = payload.length;

      if (length > BUFFER_SIZE) {
        throw new IllegalArgumentException(
            String.format("Message length %d exceeds buffer size %d", length, BUFFER_SIZE));
      }

      out.writeInt(length);
      out.write(payload, 0, length);
      out.flush();

    } catch (IOException ex) {
      throw new IllegalStateException("Send failed", ex);
    }
  }

}
