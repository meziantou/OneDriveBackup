﻿using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Threading.Channels;
using Meziantou.OneDrive;
using Meziantou.OneDrive.Windows;
using OneDriveBackup;

const int MaximumConcurrency = 8;
var onedriveFiles = Channel.CreateBounded<(string FullPath, OneDriveItem OneDriveItem)>(100);

var backupFolder = args.Length > 0 ? args[0] : "OneDriveBackup";
var localFolder = args.Length > 1 ? args[1] : null;
Console.WriteLine("Backup folder: " + backupFolder);
Console.WriteLine("Local folder: " + localFolder);
var paths = new
{
    Index = Path.GetFullPath(Path.Combine(backupFolder, "index." + DateTime.UtcNow.ToString("yyyyMMdd-HHmmssfffffff", CultureInfo.InvariantCulture) + ".json")),
    Blobs = Path.GetFullPath(Path.Combine(backupFolder, "blobs")),
    BlobsTemp = Path.GetFullPath(Path.Combine(backupFolder, "blobs", "temp")),
};

int totalItemCount = 0;
int processingItemCount = 0;
int errorCount = 0;
bool allItemFound = false;
try
{
    if (Directory.Exists(paths.BlobsTemp))
    {
        Directory.Delete(paths.BlobsTemp, recursive: true);
    }

    await Task.WhenAll(
        Task.Run(() => ListOneDriveFiles()),
        Task.Run(() => ProcessFiles()));
}
catch (Exception ex)
{
    Console.Error.WriteLine(ex);
    throw;
}

return errorCount;

async Task ListOneDriveFiles()
{
    try
    {
        // Create client
        var client = new OneDriveClient()
        {
            ApplicationId = "000000004418B915",
            AuthorizationProvider = new AuthorizationCodeProvider(),
            RefreshTokenHandler = new CredentialManagerRefreshTokenHandler("OneDriveBackup", Meziantou.Framework.Win32.CredentialPersistence.LocalMachine),
            AuthenticateOnUnauthenticatedError = true,
            HandleTooManyRequests = true,
        };
        await client.AuthenticateAsync();

        var folders = new Queue<(string FullPath, OneDriveItem Item)>();

        // Enumerate content
        var rootFolder = await RetryAsync(() => client.GetRootFolderAsync());
        folders.Enqueue(("", rootFolder));
        while (folders.TryDequeue(out var folder))
        {
            var children = await RetryAsync(() => folder.Item.GetChildrenAsync(CancellationToken.None));
            foreach (var child in children)
            {
                var fullPath = folder.FullPath + "/" + child.Name;
                if (child.File != null)
                {
                    Interlocked.Increment(ref totalItemCount);
                    await onedriveFiles.Writer.WriteAsync((fullPath, child));
                }
                else if (child.Folder != null)
                {
                    folders.Enqueue((fullPath, child));
                }
            }
        }
    }
    catch (Exception ex)
    {
        onedriveFiles.Writer.Complete(ex);
        Console.Error.WriteLine(ex);
        Interlocked.Increment(ref errorCount);
    }
    finally
    {
        _ = onedriveFiles.Writer.TryComplete();
        allItemFound = true;
    }
}

async Task ProcessFiles()
{
    await using var index = new IndexWriter(paths.Index);
    await Parallel.ForEachAsync(Enumerable.Range(0, MaximumConcurrency), async (i, cancellationToken) =>
    {
        await foreach (var (fullPath, onedriveItem) in onedriveFiles.Reader.ReadAllAsync(cancellationToken))
        {
            if (onedriveItem.File is null)
            {
                Console.Error.WriteLine($"Cannot backup file '{fullPath}', file is null");
                Interlocked.Increment(ref errorCount);
                continue;
            }

            try
            {
                var currentProcessingCount = Interlocked.Increment(ref processingItemCount);
                Console.WriteLine($"Processing ({currentProcessingCount}/{totalItemCount}{(allItemFound ? "" : "*")}) {fullPath}");

                // Download item
                Sha1Value? sha1 = onedriveItem.File.Hashes?.Sha1Hash is string value ? new Sha1Value(value) : null;
                if (sha1 != null)
                {
                    var path = GetBlobPath(sha1.Value);
                    if (!System.IO.File.Exists(path))
                    {
                        // find local file and check existing sha1
                        var handled = false;
                        if (localFolder != null)
                        {
                            var localFile = Path.Join(localFolder, fullPath);
                            if (System.IO.File.Exists(localFile))
                            {
                                var localSha1 = ComputeSha1(localFile);
                                if (localSha1 == sha1)
                                {
                                    var tempPath = Path.Combine(paths.BlobsTemp, Guid.NewGuid().ToString("N"));
                                    Directory.CreateDirectory(Path.GetDirectoryName(tempPath)!);
                                    System.IO.File.Copy(localFile, tempPath);
                                    await CopyFileToBlobs(tempPath, localSha1);
                                    handled = true;
                                }
                            }
                        }

                        // Download file from onedrive
                        if (!handled)
                        {
                            using var stream = await RetryAsync(() => onedriveItem.DownloadAsync(cancellationToken));
                            await DownloadBlobAsync(stream, sha1, onedriveItem.Size);
                        }
                    }
                }
                else
                {
                    using var stream = await RetryAsync(() => onedriveItem.DownloadAsync(cancellationToken));
                    sha1 = await DownloadBlobAsync(stream, expectedSha1: null, onedriveItem.Size);
                }

                // Write index
                await index.AddFileAsync(fullPath, sha1.Value, onedriveItem.Size, onedriveItem.CreatedDateTime, onedriveItem.LastModifiedDateTime);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
                Interlocked.Increment(ref errorCount);
                throw;
            }
        }

        Console.WriteLine("Worker " + i + " ended");
    });

    Console.WriteLine("Writing index");
}

string GetBlobPath(Sha1Value sha1)
{
    // Increase hierarchy length to avoid folders with to many files
    return Path.Combine(paths.Blobs, sha1.Value[0].ToString(), sha1.Value[1].ToString(), sha1.Value[2].ToString(), sha1.Value);
}

async Task<Sha1Value> DownloadBlobAsync(Stream stream, Sha1Value? expectedSha1, long fileSize)
{
    var tempPath = Path.Combine(paths.BlobsTemp, Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(Path.GetDirectoryName(tempPath)!);
    await using (var fs = System.IO.File.OpenWrite(tempPath))
    {
        var buffer = ArrayPool<byte>.Shared.Rent(4096);
        try
        {
            int bytesRead;
            while ((bytesRead = await stream.ReadAsync(new Memory<byte>(buffer), CancellationToken.None)) != 0)
            {
                await fs.WriteAsync(new ReadOnlyMemory<byte>(buffer, 0, bytesRead), CancellationToken.None);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    var sha1 = ComputeSha1(tempPath);
    if (expectedSha1 != null && sha1 != expectedSha1)
    {
        Interlocked.Increment(ref errorCount);
        throw new Exception("File hash differ");
    }

    await CopyFileToBlobs(tempPath, sha1);
    return sha1;
}

async Task CopyFileToBlobs(string path, Sha1Value hash)
{
    var dest = GetBlobPath(hash);
    Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
    try
    {
        await RetryAction(() => System.IO.File.Move(path, dest, overwrite: true));
    }
    catch (Exception ex)
    {
        Interlocked.Increment(ref errorCount);
        Console.Error.WriteLine($"Cannot move file to {dest}: " + ex.ToString());
    }
}

Sha1Value ComputeSha1(string filePath)
{
    using var fs = System.IO.File.OpenRead(filePath);
    using var hash = SHA1.Create();
    return new Sha1Value(Convert.ToHexString(hash.ComputeHash(fs)));
}

async ValueTask RetryAction(Action action)
{
    int count = 0;
    while (true)
    {
        try
        {
            action();
            return;
        }
        catch when (count < 5)
        {
            count++;
            await Task.Delay(1000 * count);
        }
    }
}

async Task<T> RetryAsync<T>(Func<Task<T>> action)
{
    int count = 0;
    while (true)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch when (count < 5)
        {
            count++;
            await Task.Delay(1000 * count);
        }
    }
}
