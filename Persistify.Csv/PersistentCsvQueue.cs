using Persistify.Csv.Serializer;

namespace Persistify.Csv;

public static class PersistentCsvQueue
{
	public static PersistentQueue<TItem, TabularSerializer<TItem>> Create<TItem>(
		string queueDirPath,
		int chunkSize = 4096,
		int maxChunksPerFile = 1000)
		where TItem : new()
	{
		return new(
			queueDirPath,
			new TabularSerializer<TItem>(),
			chunkSize,
			maxChunksPerFile);
	}
}