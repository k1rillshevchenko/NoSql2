using System.Diagnostics;
using MongoDB.Bson;
using MongoDB.Driver;
using Npgsql;
using StackExchange.Redis;

Console.OutputEncoding = System.Text.Encoding.UTF8;

const string pgConnStr = "Host=localhost;Port=5432;Database=lab1;Username=postgres;Password=123qwe123";
const string mongoConnStr = "mongodb://localhost:27017";
const string redisConnStr = "localhost:6379";


Console.WriteLine("ТЕСТ ШВИДКОДІЇ");


const int recordsCount = 20000;

var mongoClient = new MongoClient(mongoConnStr);
var mongoDb = mongoClient.GetDatabase("fleet_telemetry_db");
var mongoCollection = mongoDb.GetCollection<BsonDocument>("telemetry");

await mongoCollection.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty);

var mongoDocs = new List<BsonDocument>(recordsCount);
for (int i = 0; i < recordsCount; i++)
{
    mongoDocs.Add(new BsonDocument
    {
        { "vehicle_id", i % 50 + 1 },
        { "recorded_at", DateTime.UtcNow },
        { "latitude", 50.4501 + (i * 0.00005) },
        { "longitude", 30.5234 + (i * 0.00005) },
        { "speed_kmh", 60.0 + (i % 40) },
        { "sensors", new BsonDocument
            {
                { "engine_rpm", 2100 },
                { "fuel_percent", 72.5 },
                { "cargo_temp", 3.8 }
            }
        },
        { "alerts", new BsonArray
            {
                new BsonDocument { { "code", "P0101" }, { "severity", "WARNING" } }
            }
        }
    });
}

var sw = Stopwatch.StartNew();
await mongoCollection.InsertManyAsync(mongoDocs);
sw.Stop();
long mongoInsertMs = sw.ElapsedMilliseconds;
Console.WriteLine($"Масова вставка ({recordsCount} документів): {mongoInsertMs} мс");

await using var pgConn = new NpgsqlConnection(pgConnStr);
await pgConn.OpenAsync();

await using (var cleanCmd = new NpgsqlCommand("TRUNCATE vehicle_telemetry CASCADE;", pgConn))
{
    await cleanCmd.ExecuteNonQueryAsync();
}

sw.Restart();
await using (var tx = await pgConn.BeginTransactionAsync())
{
    for (int i = 0; i < recordsCount; i++)
    {
        await using var cmd1 = new NpgsqlCommand(
            "INSERT INTO vehicle_telemetry (vehicle_id, recorded_at, latitude, longitude, speed_kmh) " +
            "VALUES (@v, @t, @lat, @lng, @spd) RETURNING telemetry_id;", pgConn, tx);
        cmd1.Parameters.AddWithValue("v", i % 50 + 1);
        cmd1.Parameters.AddWithValue("t", DateTime.Now);
        cmd1.Parameters.AddWithValue("lat", 50.4501 + (i * 0.00005));
        cmd1.Parameters.AddWithValue("lng", 30.5234 + (i * 0.00005));
        cmd1.Parameters.AddWithValue("spd", 60.0 + (i % 40));
        long telemId = (long)await cmd1.ExecuteScalarAsync()!;

        await using var cmd2 = new NpgsqlCommand(
            "INSERT INTO telemetry_sensors (telemetry_id, sensor_name, sensor_value) VALUES " +
            $"({telemId}, 'engine_rpm', 2100), ({telemId}, 'fuel_percent', 72.5), ({telemId}, 'cargo_temp', 3.8);", pgConn, tx);
        await cmd2.ExecuteNonQueryAsync();
    }
    await tx.CommitAsync();
}
sw.Stop();
long pgInsertMs = sw.ElapsedMilliseconds;
Console.WriteLine($" Масова вставка ({recordsCount} пакетів): {pgInsertMs} мс");

sw.Restart();
var filter = Builders<BsonDocument>.Filter.Eq("vehicle_id", 5);
var mongoResults = await mongoCollection.Find(filter)
    .Sort(Builders<BsonDocument>.Sort.Descending("recorded_at"))
    .Limit(100)
    .ToListAsync();
sw.Stop();
long mongoReadMs = sw.ElapsedMilliseconds;
Console.WriteLine($" Читання 100 пакетів із датчиками: {mongoReadMs} мс");

sw.Restart();
const string sqlJoin = @"
    SELECT t.telemetry_id, t.recorded_at, t.speed_kmh, s.sensor_name, s.sensor_value
    FROM vehicle_telemetry t
    JOIN telemetry_sensors s ON t.telemetry_id = s.telemetry_id
    WHERE t.vehicle_id = 5
    ORDER BY t.recorded_at DESC
    LIMIT 300;";

await using var readCmd = new NpgsqlCommand(sqlJoin, pgConn);
await using var reader = await readCmd.ExecuteReaderAsync();
int rows = 0;
while (await reader.ReadAsync()) rows++;
sw.Stop();
long pgReadMs = sw.ElapsedMilliseconds;
Console.WriteLine($"Читання 100 пакетів із датчиками (через JOIN): {pgReadMs} мс\n");

Console.WriteLine("РОБОТА З REDIS");


try
{
    var redis = ConnectionMultiplexer.Connect(redisConnStr);
    var db = redis.GetDatabase();

    await db.StringSetAsync("driver:online:1", "Active", TimeSpan.FromSeconds(60));
    string? status = await db.StringGetAsync("driver:online:1");
    Console.WriteLine($"Статус водія 1: {status} (TTL: 60 сек)");

    await db.HashSetAsync("vehicle:live:1", new HashEntry[]
    {
        new("speed", "78.5"),
        new("fuel", "64.2%"),
        new("engine_status", "Normal")
    });
    var liveSpeed = await db.HashGetAsync("vehicle:live:1", "speed");
    Console.WriteLine($"Поточна швидкість авто 1: {liveSpeed} км/год");

    await db.KeyDeleteAsync("fleet:locations");
    await db.GeoAddAsync("fleet:locations", 30.5234, 50.4501, "Truck-01");
    await db.GeoAddAsync("fleet:locations", 30.5100, 50.4300, "Truck-02");
    await db.GeoAddAsync("fleet:locations", 30.7000, 50.6000, "Truck-03");

    var nearby = await db.GeoRadiusAsync("fleet:locations", 30.5234, 50.4501, 5, GeoUnit.Kilometers);
    Console.WriteLine("Автомобілі в радіусі 5 км від центру:");
    foreach (var veh in nearby)
    {
        Console.WriteLine($"Знайдено: {veh.Member}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Переконайтеся, що служба Redis запущена: {ex.Message}");
}

Console.ReadKey();