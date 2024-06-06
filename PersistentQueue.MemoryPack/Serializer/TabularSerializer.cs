using MemoryPack;

namespace PersistentQueue.MemoryPack.Serializer;

public class TabularSerializer<TItem> : ITabularSerializer<TItem>
{
	public string FileExtension => "bin";

	public List<TItem> DeserializeTable(Buffer buffer)
	{
		List<TItem> items = [];
		var payloadLengthBytes = new byte[sizeof(int)];
		byte[] payloadBytes = [];

		while (buffer.PopRange(payloadLengthBytes) == sizeof(int))
		{
			var payloadLength = MemoryPackSerializer.Deserialize<int>(payloadLengthBytes);

			if (payloadLength < 0 || payloadLength > buffer.Count)
			{
				break;
			}

			if (payloadBytes.Length < payloadLength)
			{
				payloadBytes = new byte[payloadLength];
			}

			var payloadSpan = payloadBytes.AsSpan()[..payloadLength];

			buffer.PopRange(payloadSpan);
			var item = MemoryPackSerializer.Deserialize<TItem>(payloadSpan);

			if (item == null)
			{
				break;
			}

			items.Add(item);
		}

		return items;
	}

	public byte[] SerializeHeaders()
	{
		return [];
	}

	public byte[] SerializeRow(TItem item)
	{
		byte[] payload = MemoryPackSerializer.Serialize(item);
		byte[] payloadLength = MemoryPackSerializer.Serialize(payload.Length);
		return [.. payloadLength, .. payload];
	}
}
