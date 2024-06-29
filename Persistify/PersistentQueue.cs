using System.Text.RegularExpressions;

namespace Persistify;

public class PersistentQueueWriteException(string message) : Exception(message)
{
}

public interface ITabularSerializer<TItem>
{
	string FileExtension { get; }
	byte[] SerializeHeaders();
	byte[] SerializeRow(TItem item);
	List<TItem> DeserializeTable(Buffer buffer);
}

public class PersistentQueue<TItem, TSerializer> : IPersistentQueue<TItem>
	where TItem : new()
	where TSerializer : ITabularSerializer<TItem>
{
	public int ChunkSize { get; set; }
	public int MaxChunksPerFile { get; set; }

	public void Enqueue(TItem item)
	{
		lock (syncRoot)
		{
			if (writeFile == null)
			{
				OpenWriteFile();
			}

			var bytes = serializer.SerializeRow(item);

			if (!writeFile!.TryAppend(bytes))
			{
				PushWriteFileToQueue();
				OpenWriteFile();
				if (!writeFile.TryAppend(bytes))
				{
					throw new PersistentQueueWriteException("unable to write to chunked file; most likely chunked file parameters are set too small");
				}
			}

			Monitor.Pulse(syncRoot);
		}
	}

	public bool TryPeekFile(out List<TItem>? items)
	{
		string? readFile = null;
		lock (syncRoot)
		{
			if (readFiles.Count == 0)
			{
				if (writeFile != null)
				{
					PushWriteFileToQueue();
					readFile = readFiles.Peek();
				}
			}
			else
			{
				readFile = readFiles.Peek();
			}
		}

		if (readFile == null)
		{
			items = null;
			return false;
		}

		Buffer buffer = [];
		ChunkedFile.ChunkedFile.ReadChunkedFile(readFile, serializer.FileExtension, buffer);

		items = serializer.DeserializeTable(buffer);
		return true;
	}

	public bool TryPeekFile(out List<TItem>? items, TimeSpan timeout)
	{
		lock (syncRoot)
		{
			if (TryPeekFile(out items))
			{
				return true;
			}

			if (Monitor.Wait(syncRoot, timeout))
			{
				return TryPeekFile(out items);
			}
			else
			{
				return false;
			}
		}
	}

	public void PopFile()
	{
		lock (syncRoot)
		{
			var popped = readFiles.Dequeue();
			ChunkedFile.ChunkedFile.Delete(popped);
		}
	}

	public void ReplaceFrontFile(List<TItem> items)
	{
		if (items.Count == 0)
		{
			PopFile();
			return;
		}

		var tmpFile = TmpFilePath();

		{
			using var tmpChunkedFile = new ChunkedFile.ChunkedFile(
				new ChunkedFile.ChunkedFileConfig
				{
					FilePath = tmpFile,
					ChunkSize = ChunkSize,
					MaxChunksPerFile = 999999,
					ChunkFileExtension = serializer.FileExtension,
				});

			if (!tmpChunkedFile.TryAppend(serializer.SerializeHeaders()))
			{
				throw new PersistentQueueWriteException("unable to write to chunked file; most likely chunked file parameters are set too small");
			}

			foreach (var item in items)
			{
				if (!tmpChunkedFile.TryAppend(serializer.SerializeRow(item)))
				{
					throw new PersistentQueueWriteException("unable to write to chunked file; most likely chunked file parameters are set too small");
				}
			}
		}

		lock (syncRoot)
		{
			var readFile = readFiles.Peek();
			ChunkedFile.ChunkedFile.Replace(tmpFile, readFile);
		}
	}

	public void LogUnrecoverable(List<TItem> items)
	{
		CreateUnrecoverableDirIfNotExists();

		var unrecFile = UnrecoverablePath();
		using var unrecWriteStream = File.OpenWrite(unrecFile);

		unrecWriteStream.Write(serializer.SerializeHeaders());

		foreach (var item in items)
		{
			unrecWriteStream.Write(serializer.SerializeRow(item));
		}
	}

	private void PushWriteFileToQueue()
	{
		readFiles.Enqueue(writeFile!.FilePath);
		writeFile.Dispose();
		writeFile = null;
	}

	private void OpenWriteFile()
	{
		writeFile = new ChunkedFile.ChunkedFile(
			new ChunkedFile.ChunkedFileConfig
			{
				FilePath = NextFilePath(),
				ChunkSize = ChunkSize,
				MaxChunksPerFile = MaxChunksPerFile,
				ChunkFileExtension = serializer.FileExtension,
			});

		if (!writeFile.TryAppend(serializer.SerializeHeaders()))
		{
			throw new PersistentQueueWriteException("unable to write to chunked file; most likely chunked file parameters are set too small");
		};
	}

	private string NextFilePath()
	{
		return queueDirPath + "\\" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-ffffff");
	}

	private string TmpFilePath()
	{
		return queueDirPath + "\\" + Guid.NewGuid().ToString();
	}

	private string UnrecoverablePath()
	{
		return queueDirPath + "\\unrecoverable\\" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-ffffff") + "." + serializer.FileExtension;
	}

	public PersistentQueue(string queueDirPath, TSerializer serializer, int chunkSize = 4096, int maxChunksPerFile = 1000)
	{
		ChunkSize = chunkSize;
		MaxChunksPerFile = maxChunksPerFile;
		writeFile = null;
		readFiles = new Queue<string>();
		syncRoot = new object();
		this.queueDirPath = queueDirPath;
		this.serializer = serializer;
		CreateQueueDirIfNotExists();
		LoadQueueState();
	}

	public bool IsEmpty()
	{
		lock (syncRoot)
		{
			return readFiles.Count == 0 && writeFile == null;
		}
	}

	private static bool IsValidPqfFileName(string filePath)
	{
		var rx = new Regex(
			@"\\\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-\d{6}$",
			RegexOptions.Compiled | RegexOptions.IgnoreCase);

		return rx.IsMatch(filePath);
	}

	private void LoadQueueState()
	{
		var files = Directory.GetDirectories(queueDirPath)
			.Where(IsValidPqfFileName)
			.Where(f => ChunkedFile.ChunkedFile.IsValidChunkedFile(f, serializer.FileExtension))
			.OrderBy(s => s);

		foreach (var file in files)
		{
			readFiles.Enqueue(file);
		}
	}

	private void CreateQueueDirIfNotExists()
	{
		Directory.CreateDirectory(queueDirPath);
	}

	private void CreateUnrecoverableDirIfNotExists()
	{
		Directory.CreateDirectory(queueDirPath + "\\unrecoverable");
	}

	public void Dispose()
	{
		if (writeFile != null)
		{
			writeFile.Dispose();
			writeFile = null;
		}
	}

	private readonly string queueDirPath;
	private ChunkedFile.ChunkedFile? writeFile;
	private readonly Queue<string> readFiles;
	private readonly object syncRoot;
	private readonly TSerializer serializer;
}
