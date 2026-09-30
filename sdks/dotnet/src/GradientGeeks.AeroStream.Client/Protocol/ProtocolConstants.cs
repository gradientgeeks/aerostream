namespace GradientGeeks.AeroStream.Client.Protocol;

/// <summary>
/// AeroStream native binary protocol framing constants.
/// </summary>
public static class ProtocolConstants
{
    public const byte MagicByte0 = 0xAE;
    public const byte MagicByte1 = 0x01;
    public const ushort Magic = 0xAE01;
    public const int HeaderLength = 7;
    public const int StatusPrefixLength = 3;

    // Status codes
    public const byte StatusOk = 0;
    public const byte StatusEmpty = 1;
    public const byte StatusData = 2;
    public const byte StatusAuthFailed = 3;
    public const byte StatusOutOfOrderSequenceNumber = 45;

    // Command codes
    public const byte CmdAuth = 0;
    public const byte CmdProduce = 1;
    public const byte CmdFetch = 2;
    public const byte CmdReplicaFetch = 3;
    public const byte CmdFetchMulti = 4;
}
