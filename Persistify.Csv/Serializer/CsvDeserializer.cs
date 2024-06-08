using System.Reflection;

namespace Persistify.Csv.Serializer;

internal class CsvDeserializer<T>
	where T : new()
{
	public void DeserializeHeaders(string row)
	{
		props.Clear();
		fields.Clear();

		var propAndFieldNames = row.Split(',');

		int i = 0;

		for (; i < propAndFieldNames.Length - 1; i++)
		{
			var propInfo = typeof(T).GetProperty(propAndFieldNames[i]);
			if (propInfo == null)
			{
				break;
			}
			props.Add(propInfo);
		}

		for (; i < propAndFieldNames.Length - 1; i++)
		{
			var fieldInfo = typeof(T).GetField(propAndFieldNames[i])
				?? throw new Exception($"no such field \"{propAndFieldNames[i]}\" on type \"{typeof(T)}\"");
			fields.Add(fieldInfo);
		}

		if (propAndFieldNames[^1] != "Row Checksum")
		{
			throw new Exception($"missing column \"Row Checksum\"");
		}
	}

	public T DeserializeRow(string row)
	{
		var lastComma = row.LastIndexOf(',');

		var expectedChecksum = lastComma == -1
			? Checksum.Calculate("")
			: Checksum.Calculate(row[..lastComma]);

		var actualChecksum = Checksum.FromString(row[(lastComma + 1)..]);

		if (expectedChecksum != actualChecksum)
		{
			throw new ChecksumException(
				$"invalid checksum: expected {Checksum.ToString(expectedChecksum)}" +
				$", found {Checksum.ToString(actualChecksum)}");
		}

		var propAndFieldVals = CsvElement.SplitAndUnescapeElementStrings(row);

		if (propAndFieldVals.Count != props.Count + fields.Count + 1)
		{
			throw new Exception($"expected {props.Count + fields.Count + 1} comma-separated values; found {propAndFieldVals.Count}");
		}

		var result = new T();
		var boxed = (object)result;

		for (int i = 0; i < props.Count; i++)
		{
			props[i].SetValue(
				boxed,
				CsvElement.DeserializeElement(props[i].PropertyType, propAndFieldVals[i]));
		}

		for (int i = 0; i < fields.Count; i++)
		{
			fields[i].SetValue(
				boxed,
				CsvElement.DeserializeElement(fields[i].FieldType, propAndFieldVals[i + props.Count]));
		}

		return (T)boxed;
	}

	private readonly List<PropertyInfo> props = [];
	private readonly List<FieldInfo> fields = [];
}
