namespace Persistify.Test;

internal static class Util
{
	public static DateTime NextDateTime(this Random rng)
	{
		return new DateTime(
			rng.NextInt64(
				DateTime.MinValue.Ticks,
				DateTime.MaxValue.Ticks));
	}

	public static decimal NextDecimal(this Random rng)
	{
		int numerator = rng.Next(int.MinValue, int.MaxValue);
		int denominator = rng.Next(1, int.MaxValue);
		return decimal.Divide(numerator, denominator);
	}

	public static bool AreApproximatelyEqual(object a, object b, double percentDelta)
	{
		if (a.GetType() != b.GetType())
		{
			return false;
		}

		if (a.GetType() == typeof(double))
		{
			double delta = (double)a * percentDelta * 0.01;
			return Math.Abs((double)a - (double)b) <= delta;
		}

		if (a.GetType() == typeof(DateTime))
		{
			long delta = (long)(((DateTime)a).Ticks * percentDelta * 0.01);
			return Math.Abs(((DateTime)a).Ticks - ((DateTime)b).Ticks) <= delta;
		}

		return Equals(a, b);
	}
}
