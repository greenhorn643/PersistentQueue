using System.Reflection;

namespace PersistentQueue.Csv.Serializer;

internal class CsvSerializer<T>
{
	public CsvSerializer() { }

	public string SerializeHeaders()
	{
		props.Clear();
		fields.Clear();

		foreach (var prop in typeof(T)
			.GetProperties()
			.Where(p => p.CanRead && p.CanWrite && CsvElement.IsSupportedType(p.PropertyType)))
		{
			props.Add(prop);
		}

		foreach (var field in typeof(T)
			.GetFields()
			.Where(f => f.IsPublic && !f.IsStatic && !f.IsInitOnly && CsvElement.IsSupportedType(f.FieldType)))
		{
			fields.Add(field);
		}

		return string.Join(",", props.Select(p => p.Name).Concat(fields.Select(f => f.Name)));
	}

	public string SerializeRow(T item)
	{
		return string.Join(",",
			props
			.Select(p => CsvElement.SerializeElement(p.GetValue(item)!))
			.Concat(fields
				.Select(f => CsvElement.SerializeElement(f.GetValue(item)!))));
	}

	private readonly List<PropertyInfo> props = [];
	private readonly List<FieldInfo> fields = [];
}
