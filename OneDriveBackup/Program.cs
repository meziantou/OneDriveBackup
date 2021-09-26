﻿using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Threading.Channels;
using Meziantou.OneDrive;
using Meziantou.OneDrive.Windows;
using OneDriveBackup;
using OneDriveBackup.Authentication;

const int MaximumConcurrency = 16;

var backupFolder = args.Length > 0 ? args[0] : "OneDriveBackup";
var paths = new
{
    Index = Path.GetFullPath(Path.Combine(backupFolder, "index." + DateTime.UtcNow.ToString("yyyyMMdd-HHmmssfffffff", CultureInfo.InvariantCulture) + ".json")),
    Blobs = Path.GetFullPath(Path.Combine(backupFolder, "blobs")),
    BlobsTemp = Path.GetFullPath(Path.Combine(backupFolder, "blobs", "temp")),
};

var onedriveFiles = Channel.CreateBounded<(string FullPath, OneDriveItem OneDriveItem)>(100_000);
try
{
    await Task.WhenAll(
        Task.Run(() => ListOneDriveFiles()),
        Task.Run(() => ProcessFiles()));
}
catch (Exception ex)
{
    Console.WriteLine(ex);
    throw;
}

async Task ListOneDriveFiles()
{
    try
    {
        // Create client
        var client = new OneDriveClient
        {
            ApplicationId = "000000004418B915",
            AuthorizationProvider = new EdgeView2AuthorizationCodeProvider(),
            RefreshTokenHandler = new CredentialManagerRefreshTokenHandler("OneDriveBackup", Meziantou.Framework.Win32.CredentialPersistence.LocalMachine),
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
        Console.WriteLine(ex);
    }
    finally
    {
        _ = onedriveFiles.Writer.TryComplete();
    }
}

async Task ProcessFiles()
{
    await using var index = new IndexWriter(paths.Index);
    await Parallel.ForEachAsync(Enumerable.Repeat(0, MaximumConcurrency), async (_, cancellationToken) =>
    {
        await foreach (var (fullPath, onedriveItem) in onedriveFiles.Reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                Console.WriteLine("Processing " + fullPath);

                // Download item
                Sha1Value? sha1 = onedriveItem.File.Hashes.Sha1Hash is string value ? new Sha1Value(value) : null;
                if (sha1 != null)
                {
                    var path = GetBlobPath(sha1.Value);
                    if (!System.IO.File.Exists(path))
                    {
                        using var stream = await RetryAsync(() => onedriveItem.DownloadAsync(cancellationToken));
                        await DownloadBlobAsync(stream, sha1, onedriveItem.Size);
                    }
                }
                else
                {
                    using var stream = await RetryAsync(() => onedriveItem.DownloadAsync(cancellationToken));
                    sha1 = await DownloadBlobAsync(stream, expectedSha1: null, onedriveItem.Size);
                }

                // Write index
                index.AddFile(fullPath, sha1.Value, onedriveItem.Size, onedriveItem.CreatedDateTime, onedriveItem.LastModifiedDateTime);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }
    });
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
        throw new Exception("File hash differ");
    }

    var dest = GetBlobPath(sha1);
    Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
    System.IO.File.Move(tempPath, dest, overwrite: true);
    return sha1;
}

Sha1Value ComputeSha1(string filePath)
{
    using var fs = System.IO.File.OpenRead(filePath);
    using var hash = SHA1.Create();
    return new Sha1Value(Convert.ToHexString(hash.ComputeHash(fs)));
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
            await Task.Delay(1000);
        }
    }
}