using Persistify.ChunkedFile;

namespace Persistify.Test;

[TestClass]
public class Persistify_ChunkedFile
{
	private static readonly string testDirectory = Environment.CurrentDirectory + "/Test_ChunkedFile";

	[TestInitialize]
	public void Setup()
	{
		if (Directory.Exists(testDirectory))
		{
			Directory.Delete(testDirectory, true);
		}
	}

	[TestMethod]
	public void CanCreateChunkedFile()
	{
		var config = new ChunkedFileConfig
		{
			ChunkFileExtension = ".bin",
			ChunkSize = 16,
			MaxChunksPerFile = 1,
			FilePath = testDirectory + "/chunk1"
		};

		var chunkedFile = new ChunkedFile.ChunkedFile(config);

		Assert.IsNotNull(chunkedFile);
		Assert.AreEqual(chunkedFile.ChunkFileExtension, config.ChunkFileExtension);
		Assert.AreEqual(chunkedFile.ChunkSize, config.ChunkSize);
		Assert.AreEqual(chunkedFile.MaxChunksPerFile, config.MaxChunksPerFile);
		Assert.AreEqual(chunkedFile.FilePath, config.FilePath);
	}

	[TestMethod]
	[DataRow(1, 1, 1)]
	[DataRow(2, 1, 1)]
	[DataRow(10, 10, 10)]
	[DataRow(11, 10, 10)]
	public void CanWriteToChunkedFile_OneChunk(int nBytesToWrite, int chunkSize, int maxChunksPerFile)
	{
		var rng = new Random();

		var config = new ChunkedFileConfig
		{
			ChunkFileExtension = "bin",
			ChunkSize = chunkSize,
			MaxChunksPerFile = maxChunksPerFile,
			FilePath = testDirectory + "/chunk1"
		};

		using var chunkedFile = new ChunkedFile.ChunkedFile(config);

		var bytes = new byte[nBytesToWrite];
		rng.NextBytes(bytes);

		if (nBytesToWrite > chunkSize)
		{
			Assert.ThrowsException<ChunkedFileSizeException>(
				() => chunkedFile.TryAppend(bytes));
		}
		else
		{
			Assert.IsTrue(chunkedFile.TryAppend(bytes));
		}
	}

	[TestMethod]
	[DataRow(1, 1, 1, 0)]
	[DataRow(1, 2, 1, 1)]
	[DataRow(8, 10, 10, 10)]
	[DataRow(8, 10, 10, 9)]
	[DataRow(8, 20, 16, 10)]
	[DataRow(8, 21, 16, 10)]
	public void CanWriteToChunkedFile_ManyChunks(
		int nBytesPerChunk,
		int nChunksToWrite,
		int chunkSize,
		int maxChunksPerFile)
	{
		var rng = new Random();

		var config = new ChunkedFileConfig
		{
			ChunkFileExtension = "bin",
			ChunkSize = chunkSize,
			MaxChunksPerFile = maxChunksPerFile,
			FilePath = testDirectory + "/chunk1"
		};

		using var chunkedFile = new ChunkedFile.ChunkedFile(config);

		var bytes = new byte[nBytesPerChunk];
		int chunksUsed = 0;
		int currentChunkBytesRemaining = chunkSize;

		for (int i = 0; i < nChunksToWrite; i++)
		{
			rng.NextBytes(bytes);

			if (nBytesPerChunk > chunkSize)
			{
				Assert.ThrowsException<ChunkedFileSizeException>(
					() => chunkedFile.TryAppend(bytes));
				break;
			}
			else if (chunksUsed == maxChunksPerFile)
			{
				Assert.IsFalse(chunkedFile.TryAppend(bytes));
				break;
			}
			else if (chunksUsed == maxChunksPerFile - 1 && currentChunkBytesRemaining < nBytesPerChunk)
			{
				Assert.IsFalse(chunkedFile.TryAppend(bytes));
				break;
			}
			else
			{
				Assert.IsTrue(chunkedFile.TryAppend(bytes));
				currentChunkBytesRemaining -= nBytesPerChunk;
				if (currentChunkBytesRemaining < nBytesPerChunk)
				{
					currentChunkBytesRemaining = chunkSize;
					chunksUsed++;
				}
			}
		}
	}

	[TestMethod]
	[DataRow(1, 1, 1, 1)]
	[DataRow(1000, 1000, 4096, 1000)]
	public void CanWriteAndReadChunkedFile(
		int nBytesPerChunk,
		int nChunksToWrite,
		int chunkSize,
		int maxChunksPerFile)
	{
		if (nBytesPerChunk > chunkSize)
		{
			throw new Exception("test required nChunksToWrite <= chunkSize");
		}
		if (chunkSize / nBytesPerChunk * maxChunksPerFile < nChunksToWrite)
		{
			throw new Exception("test required chunkSize / nBytesPerChunk * maxChunksPerFile >= nChunksToWrite");
		}

		var rng = new Random();
		List<byte> bytesWritten = [];

		var config = new ChunkedFileConfig
		{
			ChunkFileExtension = "bin",
			ChunkSize = chunkSize,
			MaxChunksPerFile = maxChunksPerFile,
			FilePath = testDirectory + "/chunk1"
		};

		var chunkedFile = new ChunkedFile.ChunkedFile(config);

		var bytes = new byte[nBytesPerChunk];

		for (int i = 0; i < nChunksToWrite; i++)
		{
			rng.NextBytes(bytes);
			Assert.IsTrue(chunkedFile.TryAppend(bytes));
			bytesWritten.AddRange(bytes);
		}

		chunkedFile.Dispose();

		ByteBuffer.ByteBuffer buffer = [];

		ChunkedFile.ChunkedFile.ReadChunkedFile(
			config.FilePath,
			config.ChunkFileExtension,
			buffer);

		Assert.AreEqual(bytesWritten.Count, buffer.Count);

		foreach (var (l, r) in bytesWritten.Zip(buffer))
		{
			Assert.AreEqual(l, r);
		}
	}
}
