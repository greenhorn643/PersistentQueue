using System.Reflection;

namespace PersistentQueue.Csv.Serializer;

internal class CsvDeserializer<T>
	where T : new()
{
	public void DeserializeHeaders(string row)
	{
		props.Clear();
		fields.Clear();

		var propAndFieldNames = row.Split(',');

		int i = 0;

		for (; i < propAndFieldNames.Length; i++)
		{
			var propInfo = typeof(T).GetProperty(propAndFieldNames[i]);
			if (propInfo == null)
			{
				break;
			}
			props.Add(propInfo);
		}

		for (; i < propAndFieldNames.Length; i++)
		{
			var fieldInfo = typeof(T).GetField(propAndFieldNames[i])
				?? throw new Exception($"no such field \"{propAndFieldNames[i]}\" on type \"{typeof(T)}\"");
			fields.Add(fieldInfo);
		}
	}

	public T DeserializeRow(string row)
	{
		var propAndFieldVals = CsvElement.SplitAndUnescapeElementStrings(row);

		if (propAndFieldVals.Count != props.Count + fields.Count)
		{
			throw new Exception($"expected {props.Count + fields.Count} comma-separated values; found {propAndFieldVals.Count}");
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
