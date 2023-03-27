using System.Diagnostics;
using System.Text;

namespace codefactor.Networking.Application.Models
{
    public class MessageBuffer
    {
        private byte[] _data;
        private long _size;
        private long _offset;

        #region buffer properties
        public bool IsEmpty => (_data == null) || (_size == 0);
        public byte[] Data => _data;
        public long Capacity => _data.Length;
        public long Size => _size;
        public long Offset => _offset;
        public byte this[long index] => _data[index];
        #endregion

        public MessageBuffer() { _data = new byte[0]; _size = 0; _offset = 0; }
        public MessageBuffer(long capacity) { _data = new byte[capacity]; _size = 0; _offset = 0; }
        public MessageBuffer(byte[] data) { _data = data; _size = data.Length; _offset = 0; }

        #region Message memory buffer management methods
        public Span<byte> AsSpan()
        {
            return new Span<byte>(_data, (int)_offset, (int)_size);
        }

        public override string ToString()
        {
            return ExtractString(0, _size);
        }

        public void Clear()
        {
            _size = 0;
            _offset = 0;
        }

        public string ExtractString(long offset, long size)
        {
            Debug.Assert(((offset + size) <= Size), "Invalid offset & size!");
            if ((offset + size) > Size)
                throw new ArgumentException("Invalid offset & size!", nameof(offset));

            return Encoding.UTF8.GetString(_data, (int)offset, (int)size);
        }

        public void Remove(long offset, long size)
        {
            Debug.Assert(((offset + size) <= Size), "Invalid offset & size!");
            if ((offset + size) > Size)
                throw new ArgumentException("Invalid offset & size!", nameof(offset));

            Array.Copy(_data, offset + size, _data, offset, _size - size - offset);
            _size -= size;
            if (_offset >= (offset + size))
                _offset -= size;
            else if (_offset >= offset)
            {
                _offset -= _offset - offset;
                if (_offset > Size)
                    _offset = Size;
            }
        }

        public void Reserve(long capacity)
        {
            Debug.Assert((capacity >= 0), "Invalid reserve capacity!");
            if (capacity < 0)
                throw new ArgumentException("Invalid reserve capacity!", nameof(capacity));

            if (capacity > Capacity)
            {
                byte[] data = new byte[Math.Max(capacity, 2 * Capacity)];
                Array.Copy(_data, 0, data, 0, _size);
                _data = data;
            }
        }

        public void Resize(long size)
        {
            Reserve(size);
            _size = size;
            if (_offset > _size)
                _offset = _size;
        }

        public void Shift(long offset) { _offset += offset; }

        public void Unshift(long offset) { _offset -= offset; }

        #endregion

        #region Message Buffer I/O methods
        public long Append(byte value)
        {
            Reserve(_size + 1);
            _data[_size] = value;
            _size += 1;
            return 1;
        }

        public long Append(byte[] buffer)
        {
            Reserve(_size + buffer.Length);
            Array.Copy(buffer, 0, _data, _size, buffer.Length);
            _size += buffer.Length;
            return buffer.Length;
        }

        public long Append(byte[] buffer, long offset, long size)
        {
            Reserve(_size + size);
            Array.Copy(buffer, offset, _data, _size, size);
            _size += size;
            return size;
        }

        public long Append(ReadOnlySpan<byte> buffer)
        {
            Reserve(_size + buffer.Length);
            buffer.CopyTo(new Span<byte>(_data, (int)_size, buffer.Length));
            _size += buffer.Length;
            return buffer.Length;
        }

        public long Append(MessageBuffer buffer) => Append(buffer.AsSpan());

        public long Append(string text)
        {
            int length = Encoding.UTF8.GetMaxByteCount(text.Length);
            Reserve(_size + length);
            long result = Encoding.UTF8.GetBytes(text, 0, text.Length, _data, (int)_size);
            _size += result;
            return result;
        }

        public long Append(ReadOnlySpan<char> text)
        {
            int length = Encoding.UTF8.GetMaxByteCount(text.Length);
            Reserve(_size + length);
            long result = Encoding.UTF8.GetBytes(text, new Span<byte>(_data, (int)_size, length));
            _size += result;
            return result;
        }

        #endregion
    }
}
