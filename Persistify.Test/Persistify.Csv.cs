using Nito.Collections;
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
	[DataRow(100, 1, 512, 1, 0.00001)]
	[DataRow(10000, 10, 512, 1, 0.00001)]
	[DataRow(10000, 1000, 512, 1, 0.00001)]
	[DataRow(10000, 1000, 512, 4, 0.00001)]
	[DataRow(100000, 1000, 4096, 1000, 0.00001)]
	[DataRow(1000000, 10000, 4096, 10, 0.00001)]
	[DataRow(1000000, 10000, 4096, 1000, 0.00001)]
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

	[TestMethod]
	[DataRow(100, 1, 512, 1, 0.00001)]
	[DataRow(10000, 10, 512, 1, 0.00001)]
	[DataRow(10000, 1000, 512, 1, 0.00001)]
	[DataRow(10000, 1000, 512, 4, 0.00001)]
	[DataRow(100000, 1000, 4096, 1000, 0.00001)]
	[DataRow(1000000, 10000, 4096, 10, 0.00001)]
	[DataRow(1000000, 10000, 4096, 1000, 0.00001)]
	public void CanReplaceFrontFileOfPersistentQueue(int nItems,
		int maxWrite,
		int chunkSize,
		int maxChunksPerFile,
		double maxPercentDelta)
	{
		using var q = PersistentCsvQueue.Create<ExampleData>(testDirectory, chunkSize, maxChunksPerFile);

		var rng = new Random();

		int nWritten = 0;
		Deque<ExampleData> inQueueData = [];
		List<ExampleData> retrievedFromInQueueData = [];
		List<ExampleData> retrievedData = [];

		while (nWritten < nItems)
		{
			int nToWrite = rng.Next(0, Math.Min(maxWrite, nItems - nWritten) + 1);

			var items = Enumerable.Range(0, nToWrite)
				.Select(_ => ExampleData.Random(rng))
				.ToList();

			foreach (var item in items)
			{
				q.Enqueue(item);
				inQueueData.AddToBack(item);
			}

			nWritten += nToWrite;

			if ((rng.Next() & 1) == 0)
			{
				if (q.TryPeekFile(out var frontItems))
				{
					Assert.IsNotNull(frontItems);
					retrievedData.AddRange(frontItems);
					q.PopFile();

					foreach (var item in frontItems)
					{
						var writtenItem = inQueueData.RemoveFromFront();
						Assert.IsNotNull(writtenItem);
						Assert.IsTrue(ExampleData.AreEqual(writtenItem, item, maxPercentDelta));
						retrievedFromInQueueData.Add(writtenItem);
					}
				}
				else
				{
					Assert.AreEqual(nWritten, retrievedData.Count);
				}
			}
			else
			{
				if (q.TryPeekFile(out var frontItems))
				{
					Assert.IsNotNull(frontItems);
					int nToReplace = rng.Next(0, Math.Min(maxWrite, nItems - nWritten + frontItems.Count) + 1);
					var replacementItems = Enumerable.Range(0, nToReplace)
						.Select(_ => ExampleData.Random(rng))
						.ToList();
					q.ReplaceFrontFile(replacementItems);

					foreach (var item in frontItems)
					{
						var writtenItem = inQueueData.RemoveFromFront();
						Assert.IsNotNull(writtenItem);
						Assert.IsTrue(ExampleData.AreEqual(writtenItem, item, maxPercentDelta));
					}

					replacementItems.Reverse();
					foreach (var item in replacementItems)
					{
						inQueueData.AddToFront(item);
					}

					nWritten -= frontItems.Count;
					nWritten += nToReplace;
				}
				else
				{
					Assert.AreEqual(nWritten, retrievedData.Count);
				}
			}
		}

		Assert.AreEqual(nWritten, nItems);

		while (retrievedData.Count < nWritten)
		{
			if (q.TryPeekFile(out var frontItems))
			{
				Assert.IsNotNull(frontItems);
				retrievedData.AddRange(frontItems);

				foreach (var item in frontItems)
				{
					var writtenItem = inQueueData.RemoveFromFront();
					Assert.IsNotNull(writtenItem);
					Assert.IsTrue(ExampleData.AreEqual(writtenItem, item, maxPercentDelta));
					retrievedFromInQueueData.Add(writtenItem);
				}

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

		Assert.AreEqual(0, inQueueData.Count);
		Assert.AreEqual(nWritten, retrievedFromInQueueData.Count);

		Assert.IsTrue(retrievedFromInQueueData.Zip(retrievedData).All(tup =>
			ExampleData.AreEqual(tup.First, tup.Second, maxPercentDelta)));
	}

	[TestCleanup]
	public void Teardown()
	{
		Directory.Delete(testDirectory, true);
	}
}
