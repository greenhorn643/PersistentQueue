namespace Persistify.Test;

public struct Empty { }

public struct ExampleData
{
	public string a;
	public DateTime b;
	public int c;
	public double d;
	public decimal e;

	public string A { get; set; }
	public DateTime B { get; set; }
	public int C { get; set; }
	public double D { get; set; }
	public decimal E { get; set; }

	public static ExampleData Random(Random rng)
	{
		return new ExampleData
		{
			a = rng.NextString(0, 10),
			b = rng.NextDateTime(),
			c = rng.Next(int.MinValue, int.MaxValue),
			d = rng.NextDouble(),
			e = rng.NextDecimal(),

			A = rng.NextString(0, 10),
			B = rng.NextDateTime(),
			C = rng.Next(int.MinValue, int.MaxValue),
			D = rng.NextDouble(),
			E = rng.NextDecimal(),
		};
	}

	public static bool AreEqual(ExampleData x, ExampleData y, double percentDelta)
	{
		return Util.AreApproximatelyEqual(x.a, y.a, percentDelta)
			&& Util.AreApproximatelyEqual(x.b, y.b, percentDelta)
			&& Util.AreApproximatelyEqual(x.c, y.c, percentDelta)
			&& Util.AreApproximatelyEqual(x.d, y.d, percentDelta)
			&& Util.AreApproximatelyEqual(x.e, y.e, percentDelta)

			&& Util.AreApproximatelyEqual(x.A, y.A, percentDelta)
			&& Util.AreApproximatelyEqual(x.B, y.B, percentDelta)
			&& Util.AreApproximatelyEqual(x.C, y.C, percentDelta)
			&& Util.AreApproximatelyEqual(x.D, y.D, percentDelta)
			&& Util.AreApproximatelyEqual(x.E, y.E, percentDelta);
	}
}

public class OnlyString
{
	public string Value { get; set; } = "";
}