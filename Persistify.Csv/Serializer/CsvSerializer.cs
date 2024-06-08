using System.Reflection;

namespace Persistify.Csv.Serializer;

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

		return string.Join(",", props
			.Select(p => p.Name)
			.Concat(fields.Select(f => f.Name))
			.Append("Row Checksum"));
	}

	public string SerializeRow(T item)
	{
		var dataCols = string.Join(",",
			props
			.Select(p => CsvElement.SerializeElement(p.GetValue(item)!))
			.Concat(fields
				.Select(f => CsvElement.SerializeElement(f.GetValue(item)!))));

		return dataCols == ""
			? Checksum.ToString(Checksum.Calculate(""))
			: dataCols + $",{Checksum.ToString(Checksum.Calculate(dataCols))}";
	}

	private readonly List<PropertyInfo> props = [];
	private readonly List<FieldInfo> fields = [];
}
