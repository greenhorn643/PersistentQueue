namespace Persistify;

public interface IPersistentQueue<TItem> : IDisposable
{
	void Enqueue(TItem item);
	bool TryPeekFile(out List<TItem>? items, TimeSpan timeout);
	void PopFile();
	void ReplaceFrontFile(List<TItem> items);
	void LogUnrecoverable(List<TItem> items);
	bool IsEmpty();
}
