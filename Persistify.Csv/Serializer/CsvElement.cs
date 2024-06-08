namespace Persistify.Csv.Serializer;

internal static class CsvElement
{
	public class CsvElementException(string message) : Exception(message)
	{
	}

	public static bool IsSupportedType(Type t)
	{
		if (t == typeof(string))
		{
			return true;
		}
		if (t == typeof(DateTime))
		{
			return true;
		}
		if (t == typeof(int))
		{
			return true;
		}
		if (t == typeof(double))
		{
			return true;
		}
		if (t == typeof(decimal))
		{
			return true;
		}
		return false;
	}

	public static string SerializeElement(object element)
	{
		Type t = element.GetType();
		if (!IsSupportedType(t))
		{
			throw new CsvElementException($"type \"{t.Name}\" is not supported");
		}
		if (t == typeof(string))
		{
			return EscapeCsvString((string)element);
		}
		if (t == typeof(DateTime))
		{
			return ((DateTime)element).ToString("yyyy-MM-dd HH:mm:ss.fff");
		}
		return element.ToString()!;
	}

	public static object DeserializeElement(Type t, string s)
	{
		if (t == typeof(string))
		{
			return s;
		}
		if (t == typeof(DateTime))
		{
			return DeserializeDateTime(s);
		}
		if (t == typeof(int))
		{
			return DeserializeInt(s);
		}
		if (t == typeof(double))
		{
			return DeserializeDouble(s);
		}
		if (t == typeof(decimal))
		{
			return decimal.Parse(s);
		}
		throw new CsvElementException($"type \"{t.Name}\" is not supported");
	}

	public static DateTime DeserializeDateTime(string s)
	{
		if (DateTime.TryParse(s, out DateTime dt))
		{
			return dt;
		}
		else
		{
			throw new CsvElementException($"invalid DateTime element \"{s}\"");
		}
	}

	public static int DeserializeInt(string s)
	{
		if (int.TryParse(s, out int n))
		{
			return n;
		}
		else
		{
			throw new CsvElementException($"invalid int element \"{s}\"");
		}
	}

	public static double DeserializeDouble(string s)
	{
		if (double.TryParse(s, out double f))
		{
			return f;
		}
		else
		{
			throw new CsvElementException($"invalid double element \"{s}\"");
		}
	}

	public static string EscapeCsvString(string s)
	{
		return string.Join("", s.Select(EscapeCsvChar));
	}

	public static string EscapeCsvChar(char c)
	{
		switch (c)
		{
			case '\\': return "\\\\";
			case '\n': return "\\n";
			case '\r': return "\\r";
			case ',': return "\\,";
			default: return c.ToString();
		}
	}

	public static List<string> SplitAndUnescapeElementStrings(string row)
	{
		var results = new List<string>();
		var element = "";
		bool escaped = false;

		foreach (char c in row)
		{
			if (escaped)
			{
				switch (c)
				{
					case '\\': element += '\\'; break;
					case 'n': element += '\n'; break;
					case 'r': element += '\r'; break;
					case ',': element += ','; break;
					default: throw new CsvElementException($"invalid escaped character '{c}'");
				}
				escaped = false;
			}
			else
			{
				if (c == ',')
				{
					results.Add(element);
					element = "";
				}
				else if (c == '\\')
				{
					escaped = true;
				}
				else
				{
					element += c;
				}
			}
		}

		if (escaped)
		{
			throw new CsvElementException($"invalid trailing escape character");
		}

		results.Add(element);

		return results;
	}
}
