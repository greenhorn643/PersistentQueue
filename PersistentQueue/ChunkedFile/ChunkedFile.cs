using System.Buffers;
using System.Text.RegularExpressions;

namespace PersistentQueue.ChunkedFile;

internal struct ChunkedFileConfig
{
	public string FilePath { get; set; }
	public int ChunkSize { get; set; }
	public int MaxChunksPerFile { get; set; }
	public string ChunkFileExtension { get; set; }
}

internal class ChunkedFileCorruptionException(string message) : Exception(message)
{
}

internal class ChunkedFile : IDisposable
{
	public string FilePath { get; }
	public int ChunkSize { get; }
	public int MaxChunksPerFile { get; }
	public string ChunkFileExtension { get; }

	public ChunkedFile(
		ChunkedFileConfig config)
	{
		FilePath = config.FilePath;
		ChunkSize = config.ChunkSize;
		MaxChunksPerFile = config.MaxChunksPerFile;
		ChunkFileExtension = config.ChunkFileExtension;
		currentChunkSize = 0;
		chunkIndex = 0;

		Directory.CreateDirectory(FilePath);
		writeStream = null;
	}

	public bool TryAppend(ReadOnlySpan<byte> buffer)
	{
		if (currentChunkSize + buffer.Length > ChunkSize)
		{
			if (chunkIndex >= MaxChunksPerFile - 1)
			{
				return false;
			}

			chunkIndex += 1;
			currentChunkSize = 0;

			if (writeStream != null)
			{
				writeStream.Dispose();
				writeStream = null;
			}
		}

		ExecuteWrite(buffer);
		currentChunkSize += buffer.Length;
		return true;
	}

	public static bool IsValidChunkedFile(string filePath, string chunkFileExtension)
	{
		try
		{
			return GetChunkPaths(filePath, chunkFileExtension).Length > 0;
		}
		catch
		{
			return false;
		}
	}

	public static void ReadChunkedFile(
		string filePath,
		string chunkFileExtension,
		Buffer buffer)
	{
		foreach (var path in GetChunkPaths(filePath, chunkFileExtension))
		{
			buffer.AddRange(File.ReadAllBytes(path));
		}
	}

	private static string[] GetChunkPaths(string filePath, string chunkFileExtension)
	{
		var rx = new Regex($@"\\(?<ChunkIndex>\d{{6}}){{1}}.{chunkFileExtension}$");

		var chunkIndicesAndPaths = Directory.EnumerateFiles(filePath)
			.Select(f => (rx.Match(f), f))
			.Where(p => p.Item1.Success)
			.Select(p => (index: int.Parse(p.Item1.Groups["ChunkIndex"].Value), path: p.f))
			.ToList();

		chunkIndicesAndPaths.Sort();

		if (chunkIndicesAndPaths[0].index != 0)
		{
			throw new ChunkedFileCorruptionException("first chunk must have index 000000");
		}
		if (chunkIndicesAndPaths[^1].index != chunkIndicesAndPaths.Count - 1)
		{
			throw new ChunkedFileCorruptionException("chunk indices must be contiguous");
		}

		return chunkIndicesAndPaths
			.Select(p => p.path)
			.ToArray();
	}

	public static void Delete(string filePath)
	{
		Directory.Delete(filePath, true);
	}

	private void ExecuteWrite(ReadOnlySpan<byte> buffer)
	{
		if (writeStream == null)
		{
			var writeChunkPath = FilePath + $"\\{chunkIndex:d6}.{ChunkFileExtension}";
			writeStream = File.OpenWrite(writeChunkPath);
		}

		writeStream.Write(buffer);
		writeStream.Flush();
	}

	public void Dispose()
	{
		if (writeStream != null)
		{
			writeStream.Dispose();
			writeStream = null;
		}
	}

	private int currentChunkSize;
	private int chunkIndex;
	private FileStream? writeStream;
}
