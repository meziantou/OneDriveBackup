using System.Text.Json;

namespace OneDriveBackup;

internal class IndexWriter : IAsyncDisposable
{
    private const string TemporaryExtensionName = ".tmp";

    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly Utf8JsonWriter _writer;

    public IndexWriter(string filePath)
    {
        _filePath = filePath;
        Directory.CreateDirectory(Path.GetDirectoryName(_filePath)!);
        _fileStream = new FileStream(filePath + TemporaryExtensionName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
        _writer = new Utf8JsonWriter(_fileStream, new JsonWriterOptions() { Indented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
        _writer.WriteStartObject();
        _writer.WriteNumber("Version", 1);
        _writer.WriteString("CreatedAt", DateTime.UtcNow);
        _writer.WriteStartArray("Files");
    }

    public void AddFile(string path, Sha1Value sha1, long fileSize, DateTime createdAtUtc, DateTime lastModifiedAtUtc)
    {
        lock (_writer)
        {
            _writer.WriteStartObject();
            _writer.WriteString("Path", path);
            _writer.WriteString("Sha1", sha1.Value);
            _writer.WriteNumber("Length", fileSize);
            _writer.WriteString("CreateAtUtc", createdAtUtc);
            _writer.WriteString("LastModifiedAtUtc", lastModifiedAtUtc);
            _writer.WriteEndObject();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _writer.WriteEndArray();
        _writer.WriteEndObject();

        await _writer.DisposeAsync();
        await _fileStream.DisposeAsync();
        System.IO.File.Move(_filePath + TemporaryExtensionName, _filePath, overwrite: false);
    }
}
