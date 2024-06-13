using Persistify.Csv.Serializer;

namespace Persistify.Test;

[TestClass]
public class Persistify_Csv_Serializer
{
	private static object Generate(Type dataType, Random rng)
	{
		if (dataType == typeof(Empty))
		{
			return new Empty();
		}
		else if (dataType == typeof(ExampleData))
		{
			return ExampleData.Random(rng);
		}
		else
		{
			throw new Exception($"unhandled type {dataType.Name}");
		}
	}

	private static bool AreEqual(Type dataType, object a, object b, double delta)
	{
		if (dataType == typeof(Empty))
		{
			return true;
		}
		else if (dataType == typeof(ExampleData))
		{
			return ExampleData.AreEqual((ExampleData)a, (ExampleData)b, delta);
		}
		else
		{
			throw new Exception($"unhandled type {dataType.Name}");
		}
	}

	[TestMethod]
	[DataRow(typeof(Empty), 1, 0)]
	[DataRow(typeof(ExampleData), 10000, 0.000001)]
	public void CanSerializeAndDeserialize(
		Type dataType,
		int nRows,
		double delta)
	{
		var serializerType = typeof(CsvSerializer<>).MakeGenericType(dataType)!;
		var serializer = Activator.CreateInstance(serializerType)!;
		var serializeHeaderMethod = serializerType.GetMethod("SerializeHeaders")!;
		var serializeRowMethod = serializerType.GetMethod("SerializeRow")!;

		var deserializerType = typeof(CsvDeserializer<>).MakeGenericType(dataType)!;
		var deserializer = Activator.CreateInstance(deserializerType)!;
		var deserializeHeaderMethod = deserializerType.GetMethod("DeserializeHeaders")!;
		var deserializeRowMethod = deserializerType.GetMethod("DeserializeRow")!;

		var headers = (string)serializeHeaderMethod.Invoke(
			serializer, [])!;

		deserializeHeaderMethod.Invoke(
			deserializer, [headers]);

		var rng = new Random();

		for (int i = 0; i < nRows; i++)
		{
			var data = Generate(dataType, rng);

			var row = (string)serializeRowMethod.Invoke(
				serializer, [data])!;

			var deserialized = deserializeRowMethod.Invoke(
				deserializer, [row])!;

			Assert.IsTrue(AreEqual(dataType, data, deserialized, delta));
		}
	}

	[TestMethod]
	[DataRow(100000, 0.003)]
	public void ChecksumProtectsDataIntegrity(
		int nRows,
		double maxFalsePositivePercentage)
	{
		var serializer = new CsvSerializer<ExampleData>();
		var deserializer = new CsvDeserializer<ExampleData>();

		deserializer.DeserializeHeaders(serializer.SerializeHeaders());

		var rng = new Random();

		int checksumExceptionCount = 0;

		for (int i = 0; i < nRows; i++)
		{
			var data = ExampleData.Random(rng);

			var row = serializer.SerializeRow(data);

			var lastCommaIdx = row.LastIndexOf(',');

			Assert.AreNotEqual(-1, lastCommaIdx);

			string corruptedRow =
				rng.NextString(0, 256) + row[lastCommaIdx..];

			try
			{
				deserializer.DeserializeRow(corruptedRow);
			}
			catch (ChecksumException)
			{
				checksumExceptionCount++;
			}
			catch { }
		}

		double actualFalsePositivePercentage =
			100 * (double)(nRows - checksumExceptionCount) / nRows;

		Console.WriteLine($"Checksum false positive percentage: {actualFalsePositivePercentage}");
		Assert.IsTrue(actualFalsePositivePercentage <= maxFalsePositivePercentage);
	}
}