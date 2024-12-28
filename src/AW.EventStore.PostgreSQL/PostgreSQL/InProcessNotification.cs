using System;

namespace AW.EventStore.PostgreSQL;

public class InProcessNotification : IEventStoreNotifications
{
    public event EventHandler StreamChanged;

    public void Publish(StreamChangedNotification notification)
        => StreamChanged?.Invoke(this, EventArgs.Empty);
}
