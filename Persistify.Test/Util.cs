namespace Persistify.Test;

internal static class Util
{
	public static string NextString(this Random rng, int minLength, int maxLength)
	{
		var len = rng.Next(minLength, maxLength);

		List<string> unicodeChars = [];

		int strLen = 0;
		while (strLen < len - 1)
		{
			var uc = rng.NextUnicodeChar();
			strLen += uc.Length;
			unicodeChars.Add(uc);
		}

		if (strLen == len - 1)
		{
			var uc = rng.NextUnicodeScalarChar();
			unicodeChars.Add(uc);
		}

		return string.Join("", unicodeChars);
	}

	public static string NextUnicodeChar(this Random rng)
	{
		return UnicodeCodePointToString(IntToUnicodeCodePoint(
			rng.Next(0, UnicodeValidCodePointCount)));
	}
	public static string NextUnicodeScalarChar(this Random rng)
	{
		return UnicodeCodePointToString(IntToUnicodeCodePoint(
			rng.Next(0, UnicodeValidScalarPointCount)));
	}

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

	public static readonly int UnicodeValidCodePointCount = 0x10F800;
	public static readonly int UnicodeValidScalarPointCount = 0xF800;

	public static int IntToUnicodeCodePoint(int i)
	{
		if (i < 0 || i >= 0x10F800)
		{
			throw new ArgumentOutOfRangeException(nameof(i));
		}

		if (i < 0xD800)
		{
			return i;
		}
		else
		{
			return i + 0x800;
		}
	}

	public static int UnicodeCodePointToInt(int u)
	{
		if (u < 0 || u >= 0x110000)
		{
			throw new ArgumentOutOfRangeException(nameof(u));
		}

		if (0xD800 <= u && u < 0xE000)
		{
			throw new ArgumentOutOfRangeException(nameof(u));
		}

		if (u < 0xD800)
		{
			return u;
		}
		else
		{
			return u - 0x800;
		}
	}

	public static string UnicodeCodePointToString(int u)
	{
		if (u < 0x10000)
		{
			return $"{(char)u}";
		}

		var highBits = (u - 0x10000) >> 10;
		var lowBits = (u - 0x10000) & 0x3FF;

		int highSurrogateCodePoint = highBits + 0xD800;
		int lowSurrogateCodePoint = lowBits + 0xDC00;

		return $"{(char)highSurrogateCodePoint}{(char)lowSurrogateCodePoint}";
	}

	public static int StringToUnicodePoint(string s)
	{
		if (s.Length == 1)
		{
			int u = s[0];
			if (0xD800 <= u && u < 0xE000)
			{
				throw new ArgumentOutOfRangeException(nameof(s));
			}
			return u;
		}
		else if (s.Length == 2)
		{
			int highSurrogateCodePoint = s[0];
			int lowSurrogateCodePoint = s[1];

			if (0xD800 > highSurrogateCodePoint || highSurrogateCodePoint >= 0xDC00)
			{
				throw new ArgumentOutOfRangeException(nameof(s));
			}
			if (0xDC00 > lowSurrogateCodePoint || lowSurrogateCodePoint >= 0xE000)
			{
				throw new ArgumentOutOfRangeException(nameof(s));
			}

			return 0x10000
				+ ((highSurrogateCodePoint - 0xD800) << 10)
				+ (lowSurrogateCodePoint - 0xDC00);
		}
		else
		{
			throw new ArgumentOutOfRangeException(nameof(s));
		}
	}
}
