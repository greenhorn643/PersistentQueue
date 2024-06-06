
using System.Text;

namespace PersistentQueue.Csv.Serializer;

public class TabularSerializer<TItem> : ITabularSerializer<TItem>
	where TItem : new()
{
	private readonly CsvSerializer<TItem> csvSerializer = new();
	private readonly CsvDeserializer<TItem> csvDeserializer = new();

	public string FileExtension => "csv";

	public List<TItem> DeserializeTable(Buffer buffer)
	{
		var bytes = new byte[buffer.Count];
		buffer.PopRange(bytes);

		var lines = Encoding.UTF8.GetString(bytes)
			.Split("\r\n");

		List<TItem> items = [];

		if (lines.Length > 1)
		{
			try
			{
				csvDeserializer.DeserializeHeaders(lines[0]);

				for (int i = 1; i < lines.Length; i++)
				{
					items.Add(csvDeserializer.DeserializeRow(lines[i]));
				}
			}
			catch
			{
			}
		}

		return items;
	}

	public byte[] SerializeHeaders()
	{
		return Encoding.UTF8.GetBytes(csvSerializer.SerializeHeaders());
	}

	public byte[] SerializeRow(TItem item)
	{
		return Encoding.UTF8.GetBytes("\r\n" + csvSerializer.SerializeRow(item));
	}
}
