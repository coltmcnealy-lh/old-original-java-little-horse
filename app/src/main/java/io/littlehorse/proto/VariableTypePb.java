// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: lh_proto.proto

package io.littlehorse.proto;

/**
 * Protobuf enum {@code lh_proto.VariableTypePb}
 */
public enum VariableTypePb
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>STRING = 0;</code>
   */
  STRING(0),
  /**
   * <code>INT = 1;</code>
   */
  INT(1),
  /**
   * <code>FLOAT = 2;</code>
   */
  FLOAT(2),
  /**
   * <code>BOOLEAN = 3;</code>
   */
  BOOLEAN(3),
  /**
   * <code>ARRAY = 4;</code>
   */
  ARRAY(4),
  /**
   * <code>OBJECT = 5;</code>
   */
  OBJECT(5),
  UNRECOGNIZED(-1),
  ;

  /**
   * <code>STRING = 0;</code>
   */
  public static final int STRING_VALUE = 0;
  /**
   * <code>INT = 1;</code>
   */
  public static final int INT_VALUE = 1;
  /**
   * <code>FLOAT = 2;</code>
   */
  public static final int FLOAT_VALUE = 2;
  /**
   * <code>BOOLEAN = 3;</code>
   */
  public static final int BOOLEAN_VALUE = 3;
  /**
   * <code>ARRAY = 4;</code>
   */
  public static final int ARRAY_VALUE = 4;
  /**
   * <code>OBJECT = 5;</code>
   */
  public static final int OBJECT_VALUE = 5;


  public final int getNumber() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalArgumentException(
          "Can't get the number of an unknown enum value.");
    }
    return value;
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static VariableTypePb valueOf(int value) {
    return forNumber(value);
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   */
  public static VariableTypePb forNumber(int value) {
    switch (value) {
      case 0: return STRING;
      case 1: return INT;
      case 2: return FLOAT;
      case 3: return BOOLEAN;
      case 4: return ARRAY;
      case 5: return OBJECT;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<VariableTypePb>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      VariableTypePb> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<VariableTypePb>() {
          public VariableTypePb findValueByNumber(int number) {
            return VariableTypePb.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalStateException(
          "Can't get the descriptor of an unrecognized enum value.");
    }
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return io.littlehorse.proto.LhProto.getDescriptor().getEnumTypes().get(0);
  }

  private static final VariableTypePb[] VALUES = values();

  public static VariableTypePb valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    if (desc.getIndex() == -1) {
      return UNRECOGNIZED;
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private VariableTypePb(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:lh_proto.VariableTypePb)
}

