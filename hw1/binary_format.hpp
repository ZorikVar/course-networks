#include <vector>
#include <string>
#include <stdint.h>

using Bytes = std::string;
using uchar = unsigned char;
static void append(Bytes& lhs, Bytes& rhs)
{
    lhs.insert(lhs.end(), rhs.begin(), rhs.end());
}

class Encoder {
public:
    Encoder()
    { }

    Bytes load()
    {
        return buffer;
    }

    void int8(uint8_t n)
    {
        buffer.push_back(uchar(n & 0xff));
    }

    void int32(uint32_t n)
    {
        for (int i = 0; i < 32; i += 8)
            buffer.push_back(uchar((n >> i) & 0xff));
    }

    void raw_str(Bytes s)
    {
        append(buffer, s);
    }

private:
    Bytes buffer;
};

class Decoder {
public:
    Decoder(Bytes data)
        : buffer(data)
    { }

    uint8_t int8()
    {
        if (pos + 1 > buffer.size())
            throw std::string("not enough input bytes");

        uint8_t parsed = (uchar)buffer[pos++];
        return parsed;
    }

    uint32_t int32()
    {
        if (pos + 4 > buffer.size())
            throw std::string("not enough input bytes");

        uint32_t parsed = 0;
        for (int i = 0; i < 32; i += 8)
            parsed |= (uchar)buffer[pos++] << i;
        return parsed;
    }

    Bytes raw_str(int n)
    {
        if (pos + n > buffer.size())
            throw std::string("not enough input bytes");

        Bytes parsed(buffer.begin() + pos, buffer.begin() + pos + n);
        pos += n;
        return parsed;
    }

private:
    Bytes buffer;
    size_t pos = 0;
};
