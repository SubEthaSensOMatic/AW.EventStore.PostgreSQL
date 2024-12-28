using AW.EventStore.PostgreSQL;
using AW.Identifiers;
using AW.NamedTypes;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DemoApp;

[NamedType("person-created-event")]
public record PersonCreatedEvent(Urn Id, string Name, string EMail);

[NamedType("person-email-changed-event")]
public record PersonEMailChangedEvent(Urn Id, string EMail);

[NamedType(PERSON_TYPE)]
public record Person(Urn Id, string Name, string EMail)
{
    public const string PERSON_TYPE = "person";
}

internal class Program
{
    static async Task Main(string[] args)
    {
        TypeRegistry.Instance.AutoRegisterTypes();

        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
        });

        var logger = loggerFactory.CreateLogger<Program>();

        await using var dataSource =
            new NpgsqlDataSourceBuilder(
                "Host=localhost;Username=postgres;Password=postgres;Database=postgres")
            .Build();

        var provider = new SimpleDataSourceProvider(dataSource);

        var config = new StoreConfiguration();

        var notification = new InProcessNotification();

        var init = new EventStoreInitializer(
            loggerFactory.CreateLogger<EventStoreInitializer>(), config, provider);

        await init.Initialize(CancellationToken.None);

        var store = new EventStore(provider, config, notification);

        var personId = Urn.CreateFromFlake(
            FlakeFactory.Instance.NewFlake(), ["aw", "demo", Person.PERSON_TYPE]);

        await store.CreateStream(
            personId.ToString(), Person.PERSON_TYPE, [new PersonCreatedEvent(personId, "Zaphod Beeblebrox", "zaphod@beeblebrox.com")], "admin");

        await store.AppendToStream(
            personId.ToString(), 1, [new PersonEMailChangedEvent(personId, "zaphod2@beeblebrox.com")], "admin");


        var events = await store.LoadStream(personId.ToString(), CancellationToken.None);

        var snapshot = new Person(Urn.Empty, string.Empty, string.Empty);

        foreach (var @event in events)
        {
            if (@event.EventPayload is PersonCreatedEvent pce)
                snapshot = snapshot with { Id = pce.Id, Name = pce.Name, EMail = pce.EMail };
            else if (@event.EventPayload is PersonEMailChangedEvent pec)
                snapshot = snapshot with { EMail = pec.EMail };

            logger.LogInformation("Event: {@event}", @event);
        }

        logger.LogInformation("Snapshot before save: {snapshot}", snapshot);

        await store.CreateSnapshot(personId.ToString(), 1, snapshot);

        var loadedSnapshot = await store.LoadSnapshot(personId.ToString());

        logger.LogInformation("Snapshot after load: {loadedSnapshot}", loadedSnapshot);

        var allEvents = await store.StreamEvents(null, 1000, CancellationToken.None);
        foreach (var @event in allEvents)
        {
            logger.LogInformation("Event: {@event}", @event);
        }
    }
}
