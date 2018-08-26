import 'dart:typed_data';

/// The CQL binary protocol is a frame based protocol. Frames are defined as:
///
///   0         8        16        24        32         40
///   +---------+---------+---------+---------+---------+
///   | version |  flags  |      stream       | opcode  |
///   +---------+---------+---------+---------+---------+
///   |                length                 |
///   +---------+---------+---------+---------+
///   |                                       |
///   .            ...  body ...              .
///   .                                       .
///   .                                       .
///   +----------------------------------------
///
/// The protocol is big-endian (network byte order).
///
/// Each frame contains a fixed size header (9 bytes) followed by a variable size
/// body. The header is described in Section 2. The content of the body depends
/// on the header opcode value (the body can in particular be empty for some
/// opcode values). The list of allowed opcodes is defined in Section 2.4 and the
/// details of each corresponding message are described Section 4.
///
/// The protocol distinguishes two types of frames: requests and responses. Requests
/// are those frames sent by the client to the server. Responses are those frames sent
/// by the server to the client. Note, however, that the protocol supports server pushes
/// (events) so a response does not necessarily come right after a client request.
///
/// Note to client implementors: client libraries should always assume that the
/// body of a given frame may contain more data than what is described in this
/// document. It will however always be safe to ignore the remainder of the frame
/// body in such cases. The reason is that this may enable extending the protocol
/// with optional features without needing to change the protocol version.
class CqlFrameHeader {
  final ByteData byteData;

  CqlFrameHeaderVersion _version;
  CqlFrameHeaderFlags _flags;
  int _streamId, _bodyLength;

  CqlFrameHeader(this.byteData);

  /// Indicates both the direction of the message and the protocol version.
  CqlFrameHeaderVersion get version {
    return _version ??= new CqlFrameHeaderVersion(byteData.getUint8(0));
  }

  void set version(CqlFrameHeaderVersion value) {
    _version = value;
    byteData.setUint8(0, value._byte);
  }

  /// Flags applying to this frame.
  CqlFrameHeaderFlags get flags {
    return _flags ??= new CqlFrameHeaderFlags(byteData.getUint8(1));
  }

  void set flags(CqlFrameHeaderFlags value) {
    _flags = value;
    byteData.setUint16(1, value.flags, Endian.big);
  }

  int get streamId {
    return _streamId ??= byteData.getUint16(2, Endian.big);
  }

  void set streamId(int value) {
    _streamId = value;
    byteData.setUint16(2, value, Endian.big);
  }

  CqlFrameOpcode get opcode {
    var byte = byteData.getUint8(4);

    if (byte < 0 || byte == 4 || byte >= CqlFrameOpcode.values.length) {
      throw new FormatException(
          'Invalid opcode for CQL binary frame; found $byte, expected integer from 0x00 to 0x10, which must not be 0x04.');
    }

    return CqlFrameOpcode.values[byte];
  }

  void set opcode(CqlFrameOpcode value) {
    byteData.setUint8(4, value.index);
  }

  int get bodyLength {
    return _bodyLength ??= byteData.getInt32(5, Endian.big);
  }

  void set bodyLength(int value) {
    _bodyLength = value;
    byteData.setInt32(5, value, Endian.big);
  }
}

/// The version is a single byte that indicates both the direction of the message
/// (request or response) and the version of the protocol in use. The most
/// significant bit of version is used to define the direction of the message:
/// `0` indicates a request, `1` indicates a response. This can be useful for protocol
/// analyzers to distinguish the nature of the packet from the direction in which
/// it is moving. The rest of that byte is the protocol version (`4` for the protocol
/// defined in this document). In other words, for this version of the protocol,
/// version will be one of:
/// * `0x04`   Request frame for this protocol version
/// * `0x84`    Response frame for this protocol version
/// Please note that while every message ships with the version, only one version
/// of messages is accepted on a given connection. In other words, the first message
/// exchanged (`STARTUP`) sets the version for the connection for the lifetime of this
/// connection.
class CqlFrameHeaderVersion {
  final int _byte;

  const CqlFrameHeaderVersion(this._byte);

  static const CqlFrameHeaderVersion requestV5 =
      const CqlFrameHeaderVersion(0x05);

  static const CqlFrameHeaderVersion responseV5 =
      const CqlFrameHeaderVersion(0x85);

  int get value => (_byte & 0x05) << 4;

  bool get isRequest => !isResponse;

  bool get isResponse => (_byte & 0x80) == 0x80;
}

/// Flags applying to a [CqlFrameHeader].
class CqlFrameHeaderFlags {
  final int flags;

  const CqlFrameHeaderFlags(this.flags);

  bool _hasFlag(int flag) => (flags & flag) == flag;

  /// If set, the frame body is compressed.
  /// The actual compression to use should have been set up beforehand through the
  /// `Startup` message (which thus cannot be compressed; Section 4.1.1).
  bool get isCompressed => _hasFlag(0x01);

  /// For a request frame, this indicates the client requires
  /// tracing of the request. Note that only QUERY, PREPARE and EXECUTE queries
  /// support tracing. Other requests will simply ignore the tracing flag if
  /// set. If a request supports tracing and the tracing flag is set, the response
  /// to this request will have the tracing flag set and contain tracing
  /// information.
  /// If a response frame has the tracing flag set, its body contains
  /// a tracing ID. The tracing ID is a [uuid] and is the first thing in
  /// the frame body. The rest of the body will then be the usual body
  /// corresponding to the response opcode.
  bool get requiresTracing => _hasFlag(0x02);

  /// For a request or response frame, this indicates
  /// that a generic key-value custom payload for a custom QueryHandler
  /// implementation is present in the frame. Such a custom payload is simply
  /// ignored by the default QueryHandler implementation.
  /// Currently, only `QUERY`, `PREPARE`, `EXECUTE` and `BATCH` requests support
  /// payload.
  /// Type of custom payload is [bytes map] (see below).
  bool get hasCustomPayload => _hasFlag(0x04);

  /// The response contains warnings which were generated by the
  /// server to go along with this response.
  /// If a response frame has the warning flag set, its body will contain the
  /// text of the warnings. The warnings are a [string list] and will be the
  /// first value in the frame body if the tracing flag is not set, or directly
  /// after the tracing ID if it is.
  bool get hasWarning => _hasFlag(0x08);

  /// Indicates that the client opts in to use protocol version
  /// that is currently in beta. Server will respond with ERROR if protocol
  /// version is marked as beta on server and client does not provide this flag.
  bool get isBeta => _hasFlag(0x10);
}

/// The various types of frame in the CQL binary protocol.
enum CqlFrameOpcode {
  error,
  startup,
  ready,
  authenticate,
  _nonExistent,
  options,
  supported,
  query,
  result,
  prepare,
  execute,
  register,
  event,
  batch,
  authChallenge,
  authResponse,
  authSuccess
}
