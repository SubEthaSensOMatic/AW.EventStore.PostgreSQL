using Npgsql;
using System;
using System.Threading.Tasks;

namespace AW.EventStore.PostgreSQL;

public class SimpleDataSourceProvider : IEventStoreDataSourceProvider
{
    private readonly NpgsqlDataSource _dataSource;

    public SimpleDataSourceProvider(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource, nameof(dataSource));
        _dataSource = dataSource;
    }

    public Task<NpgsqlDataSource> GetDataSource()
        => Task.FromResult(_dataSource);
}
