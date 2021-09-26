internal struct Sha1Value : IEquatable<Sha1Value>
{
    public Sha1Value(string value)
    {
        Value = value?.ToLowerInvariant() ?? throw new ArgumentNullException(nameof(value));
    }

    public string Value { get; }

    public override bool Equals(object? obj)
    {
        return obj is Sha1Value value && Equals(value);
    }

    public bool Equals(Sha1Value other)
    {
        return string.Equals(Value, other.Value, StringComparison.Ordinal);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Value);
    }

    public static bool operator ==(Sha1Value left, Sha1Value right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(Sha1Value left, Sha1Value right)
    {
        return !(left == right);
    }

    public override string ToString()
    {
        return Value;
    }
}