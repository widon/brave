package brave.propagation.tracecontext;

import brave.internal.Nullable;
import brave.propagation.TraceContext;
import java.util.logging.Logger;

import static brave.internal.HexCodec.lenientLowerHexToUnsignedLong;
import static brave.internal.HexCodec.writeHexLong;

final class TraceparentFormat {
  static final Logger logger = Logger.getLogger(TraceparentFormat.class.getName());
  static final int FORMAT_LENGTH = 55;

  static String writeTraceparentFormat(TraceContext context) {
    // 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
    char[] result = new char[FORMAT_LENGTH];
    result[0] = '0'; // version
    result[1] = '0'; // version
    result[2] = '-'; // delimiter
    writeHexLong(result, 3, context.traceIdHigh());
    writeHexLong(result, 19, context.traceId());
    result[35] = '-'; // delimiter
    writeHexLong(result, 36, context.spanId());
    result[52] = '-'; // delimiter
    result[53] = '0'; // options
    result[54] = context.sampled() != null && context.sampled() ? '1' : '0'; // options
    return new String(result);
  }

  /** returns the count of valid characters read from the input position */
  static int validateFormat(CharSequence parent, int pos) {
    int length = Math.max(parent.length() - pos, FORMAT_LENGTH);
    if (length < FORMAT_LENGTH) {
      logger.fine("Bad length.");
      return 0;
    }

    for (int i = 0; i < length; i++) {
      char c = parent.charAt(i + pos);
      if (c == '-') {
        // There are delimiters separating the version, trace ID, span ID and options fields.
        if (i != 2 && i != 35 && i != 52) {
          logger.fine("Expected hyphen at " + (i + pos));
          return i;
        }
        // Everything else is hex
      } else if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
        logger.fine("Expected lower hex at " + (i + pos));
        return i;
      }
    }
    return length;
  }

  static @Nullable TraceContext maybeExtractParent(CharSequence parent, int pos) {
    int version = parseUnsigned16BitLowerHex(parent, pos);
    if (version == -1) {
      logger.fine("Malformed version.");
      return null;
    }
    if (version != 0) {
      logger.fine("Unsupported version.");
      return null;
    }

    long traceIdHigh = lenientLowerHexToUnsignedLong(parent, pos + 3, pos + 19);
    long traceId = lenientLowerHexToUnsignedLong(parent, pos + 19, pos + 35);
    if (traceIdHigh == 0L && traceId == 0L) {
      logger.fine("Invalid input: expected non-zero trace ID");
      return null;
    }

    long spanId = lenientLowerHexToUnsignedLong(parent, pos + 36, pos + 52);
    if (spanId == 0L) {
      logger.fine("Invalid input: expected non-zero span ID");
      return null;
    }

    int traceOptions = parseUnsigned16BitLowerHex(parent, pos + 53);
    if (traceOptions == -1) {
      logger.fine("Malformed trace options.");
      return null;
    }

    // TODO: treat it as a bitset?
    // https://github.com/w3c/distributed-tracing/issues/8#issuecomment-382958021
    // TODO handle deferred decision https://github.com/w3c/distributed-tracing/issues/8
    boolean sampled = (traceOptions & 1) == 1;

    return TraceContext.newBuilder()
        .traceIdHigh(traceIdHigh)
        .traceId(traceId)
        .spanId(spanId)
        .sampled(sampled)
        .build();
  }

  /** Returns -1 if it wasn't hex */
  static int parseUnsigned16BitLowerHex(CharSequence lowerHex, int pos) {
    int result = 0;
    for (int i = 0; i < 2; i++) {
      char c = lowerHex.charAt(pos + i);
      result <<= 4;
      if (c >= '0' && c <= '9') {
        result |= c - '0';
      } else if (c >= 'a' && c <= 'f') {
        result |= c - 'a' + 10;
      } else {
        return -1;
      }
    }
    return result;
  }
}
