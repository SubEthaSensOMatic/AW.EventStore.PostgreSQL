namespace AW.EventStore.PostgreSQL;

public class EventIdSerializer : IEventIdSerializer
{
    public IEventId? Deserialize(byte[] eventId)
        => new EventId(eventId);

    public byte[] Serialize(IEventId eventId)
        => eventId is EventId pgId ? pgId.GetBytes() : [];
}
