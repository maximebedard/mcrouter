namespace facebook {
namespace memcache {
namespace binary_protocol {
  static constexpr uint64_t HeaderLength = 24;

  typedef struct RequestHeader {
    uint8_t  magic;
    uint8_t  opCode;
    uint16_t keyLen;
    uint8_t  extrasLen;
    uint8_t  dataType;
    uint16_t vBucketId;
    uint32_t totalBodyLen;
    uint32_t opaque;
    uint32_t cas;
  } __attribute__((__packed__)) RequestHeader_t;

  typedef struct ResponseHeader {
    uint8_t  magic;
    uint8_t  opCode;
    uint16_t keyLen;
    uint8_t  extrasLen;
    uint8_t  dataType;
    uint16_t status;
    uint32_t totalBodyLen;
    uint32_t opaque;
    uint32_t cas;
  } __attribute__((__packed__)) ResponseHeader_t;

  typedef struct SetExtras {
    uint32_t flags;
    uint32_t exptime;
  } __attribute__((__packed__)) SetExtras_t;

  typedef struct ArithExtras {
    uint32_t delta;
    uint32_t initialValue;
    uint32_t exptime;
  } __attribute__((__packed__)) ArithExtras_t;

  typedef struct TouchExtras {
    uint32_t exptime;
  } __attribute__((__packed__)) TouchExtras_t;

  typedef struct FlushExtras {
    uint32_t exptime;
  } __attribute__((__packed__)) FlushExtras_t;
}
}
}
