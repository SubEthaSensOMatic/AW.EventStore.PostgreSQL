using System;

namespace AW.EventStore.PostgreSQL;

internal class EventId : IEventId
{
    public long Sequence { get => _value.Sequence; }

    public ulong TxId { get => _value.TxId; }

    private readonly (ulong TxId, long Sequence) _value;

    public EventId(ulong txid, long sequence)
        => _value = (txid, sequence);

    public EventId(byte[] bytes)
    {
        if (bytes == null || bytes.Length != 16)
            throw new InvalidOperationException("Invalid byte count.");

        var txid =
            ((ulong)bytes[0] << 56)
            | ((ulong)bytes[1] << 48)
            | ((ulong)bytes[2] << 40)
            | ((ulong)bytes[3] << 32)
            | ((ulong)bytes[4] << 24)
            | ((ulong)bytes[5] << 16)
            | ((ulong)bytes[6] << 8)
            | bytes[7];

        var sequence =
            ((long)bytes[8] << 56)
            | ((long)bytes[9] << 48)
            | ((long)bytes[10] << 40)
            | ((long)bytes[11] << 32)
            | ((long)bytes[12] << 24)
            | ((long)bytes[13] << 16)
            | ((long)bytes[14] << 8)
            | bytes[15];

        _value = (txid, sequence);
    }

    public override int GetHashCode()
        => _value.GetHashCode();
    
    public override bool Equals(object? obj)
        => _value.Equals(obj);

    public int CompareTo(object? obj)
        => CompareTo((IEventId?)obj);
    
    public int CompareTo(IEventId? other) => other != null && other is EventId eventId
        ? _value.CompareTo(eventId._value)
        : -1;
    
    public bool Equals(IEventId? other) => other != null && other is EventId eventId
        ? _value.Equals(eventId._value)
        : false;

    public override string ToString()
        => Convert.ToHexString(GetBytes());

    public byte[] GetBytes() =>
    [
        (byte)(_value.TxId >> 56),
        (byte)(_value.TxId >> 48),
        (byte)(_value.TxId >> 40),
        (byte)(_value.TxId >> 32),
        (byte)(_value.TxId >> 24),
        (byte)(_value.TxId >> 16),
        (byte)(_value.TxId >> 8),
        (byte)_value.TxId,
        (byte)(_value.Sequence >> 56),
        (byte)(_value.Sequence >> 48),
        (byte)(_value.Sequence >> 40),
        (byte)(_value.Sequence >> 32),
        (byte)(_value.Sequence >> 24),
        (byte)(_value.Sequence >> 16),
        (byte)(_value.Sequence >> 8),
        (byte)_value.Sequence
    ];
}
