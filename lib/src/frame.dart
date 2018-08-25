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

  CqlFrameHeader(this.byteData);

  CqlFrameHeaderVersion get version {
    return _version ??= new CqlFrameHeaderVersion(byteData.getUint8(0));
  }

  CqlFrameHeaderFlags get flags {
    return _flags ??= new CqlFrameHeaderFlags(byteData.getUint8(0));
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
  final int byte;

  CqlFrameHeaderVersion(this.byte);

  int get _msb => byte << 7;

  bool get isRequest => _msb == 0x04;

  bool get isResponse => _msb == 0x84;
}

/// Flags applying to this frame.
class CqlFrameHeaderFlags {
  final int byte;

  CqlFrameHeaderFlags(this.byte);

  bool _hasFlag(int flag) => (byte & flag) == flag;

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
}
