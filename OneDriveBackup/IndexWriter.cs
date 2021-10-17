using System.Text.Json;
using System.Threading;

namespace OneDriveBackup;

internal class IndexWriter : IAsyncDisposable
{
    private const string TemporaryExtensionName = ".tmp";

    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly Utf8JsonWriter _writer;
    private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

    public IndexWriter(string filePath)
    {
        _filePath = filePath;
        Directory.CreateDirectory(Path.GetDirectoryName(_filePath)!);
        _fileStream = new FileStream(filePath + TemporaryExtensionName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, bufferSize: 0);
        _writer = new Utf8JsonWriter(_fileStream, new JsonWriterOptions() { Indented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
        _writer.WriteStartObject();
        _writer.WriteNumber("Version", 1);
        _writer.WriteString("CreatedAt", DateTime.UtcNow);
        _writer.WriteStartArray("Files");
    }

    public async Task AddFileAsync(string path, Sha1Value sha1, long fileSize, DateTime createdAtUtc, DateTime lastModifiedAtUtc)
    {
        await _semaphoreSlim.WaitAsync();
        try
        {
            _writer.WriteStartObject();
            _writer.WriteString("Path", path);
            _writer.WriteString("Sha1", sha1.Value);
            _writer.WriteNumber("Length", fileSize);
            _writer.WriteString("CreateAtUtc", createdAtUtc);
            _writer.WriteString("LastModifiedAtUtc", lastModifiedAtUtc);
            _writer.WriteEndObject();
            await _writer.FlushAsync();
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _writer.WriteEndArray();
        _writer.WriteEndObject();

        await _writer.DisposeAsync();
        await _fileStream.DisposeAsync();
        _semaphoreSlim.Dispose();
        File.Move(_filePath + TemporaryExtensionName, _filePath, overwrite: false);
    }
}
