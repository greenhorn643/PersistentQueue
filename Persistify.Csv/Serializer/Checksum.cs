namespace Persistify.Csv.Serializer;

internal class ChecksumException(string message) : Exception(message) { }

internal static class Checksum
{
	public static ushort Calculate(string rowBeforeChecksumCol)
	{
		uint dataColsSum = 0;
		foreach (char c in rowBeforeChecksumCol)
		{
			dataColsSum += c;
		}

		return (ushort)dataColsSum;
	}

	public static string ToString(ushort checksum)
	{
		return Convert.ToHexString([
			(byte)checksum,
			(byte)(checksum >> 8),
			]);
	}

	public static ushort FromString(string s)
	{
		try
		{
			var bytes = Convert.FromHexString(s);

			if (bytes.Length != 2)
			{
				throw new ChecksumException(
					$"invalid checksum string: expected 2 bytes, found {bytes.Length}");
			}

			return (ushort)(
				bytes[0]
				| (bytes[1] << 8));
		}
		catch (FormatException)
		{
			throw new ChecksumException("invalid checksum format");
		}
	}
}
