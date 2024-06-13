using Persistify.Csv;

namespace Persistify.Test;

[TestClass]
public class Persistify_Csv
{
	private static readonly string testDirectory = Environment.CurrentDirectory + "/Test_CsvQueue";

	[TestInitialize]
	public void Setup()
	{
		if (Directory.Exists(testDirectory))
		{
			Directory.Delete(testDirectory, true);
		}
	}

	[TestMethod]
	[DataRow(100, 1, 512, 1, 0.000001)]
	[DataRow(10000, 10, 512, 1, 0.000001)]
	[DataRow(10000, 1000, 512, 1, 0.000001)]
	[DataRow(10000, 1000, 512, 4, 0.000001)]
	[DataRow(100000, 1000, 4096, 1000, 0.000001)]
	[DataRow(1000000, 10000, 4096, 10, 0.000001)]
	[DataRow(1000000, 10000, 4096, 1000, 0.000001)]
	public void CanWriteAndReadFromPersistentQueue(
		int nItems,
		int maxWrite,
		int chunkSize,
		int maxChunksPerFile,
		double maxPercentDelta)
	{
		using var q = PersistentCsvQueue.Create<ExampleData>(testDirectory, chunkSize, maxChunksPerFile);

		int nWritten = 0;

		var rng = new Random();

		List<ExampleData> writtenData = [];
		List<ExampleData> retrievedData = [];

		while (nWritten < nItems)
		{
			int nToWrite = rng.Next(0, Math.Min(nItems - nWritten, maxWrite) + 1);

			var items = Enumerable.Range(0, nToWrite)
				.Select(_ => ExampleData.Random(rng))
				.ToList();

			writtenData.AddRange(items);

			foreach (var item in items)
			{
				q.Enqueue(item);
			}

			nWritten += nToWrite;

			if (q.TryPeekFile(out var frontItems))
			{
				Assert.IsNotNull(frontItems);
				retrievedData.AddRange(frontItems);
				q.PopFile();
			}
			else
			{
				Assert.IsNull(frontItems);
				Assert.AreEqual(retrievedData.Count, nWritten);
			}
		}

		while (retrievedData.Count < nWritten)
		{
			if (q.TryPeekFile(out var frontItems))
			{
				Assert.IsNotNull(frontItems);
				retrievedData.AddRange(frontItems);
				q.PopFile();
			}
			else
			{
				Assert.Fail();
			}
		}

		Assert.AreEqual(retrievedData.Count, nWritten);

		{
			Assert.IsFalse(q.TryPeekFile(out var frontItems));
			Assert.IsNull(frontItems);
		}

		Assert.IsTrue(writtenData.Zip(retrievedData).All(tup =>
			ExampleData.AreEqual(tup.First, tup.Second, maxPercentDelta)));
	}

	[TestCleanup]
	public void Teardown()
	{
		Directory.Delete(testDirectory, true);
	}
}
