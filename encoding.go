package hadoop

import "io"
import "encoding/binary"

func ReadByte(r io.Reader) (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}
func WriteByte(w io.Writer, b byte) error {
	var buf [1]byte
	buf[0] = b
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	return nil
}

func DecodeVIntSize(value byte) int {
	if value <= 127 || 144 <= value {
		return 1
	} else if 127 < value && value < 136 {
		return -int(value) + 137
	} else {
		return -int(value) + 145
	}
}

func IsNegativeVInt(fst byte) bool {
	return (127 < fst && fst < 136) || (144 <= fst)
}

func ReadVLong(r io.Reader) (int64, error) {
	fst, err := ReadByte(r)
	if err != nil {
		return 0, err
	}
	len := DecodeVIntSize(fst)
	if len == 1 {
		return int64(fst), nil
	}
	var result int64 = 0
	for idx := 0; idx < len-1; idx++ {
		b, err := ReadByte(r)
		if err != nil {
			return 0, err
		}
		result = result << 8
		result = result | int64(b&0xff)
	}
	if IsNegativeVInt(fst) {
		return result ^ -1, nil
	} else {
		return result, nil
	}
}

// Ported from https://hadoop.apache.org/docs/r2.6.2/api/src-html/org/apache/hadoop/io/WritableUtils.html#line.271
func WriteVLong(w io.Writer, i int64) (int, error) {
	nn := 0
	if i >= -112 && i <= 127 {
		err := WriteByte(w, byte(i))
		if err != nil {
			return nn, err
		}
		nn++
		return nn, nil
	}
	var length int64 = -112
	if i < 0 {
		i = i ^ -1 // take one's complement'
		length = -120
	}
	// From here on, we're dealing only with positive integers
	var iUnsigned = uint64(i)
	var tmp = iUnsigned
	for tmp != 0 {
		tmp = tmp >> 8
		length--
	}
	err := WriteByte(w, byte(length))
	if err != nil {
		return nn, err
	}
	nn++
	if length < -120 {
		length = -(length + 120)
	} else {
		length = -(length + 112)
	}
	var lengthUnsigned = uint64(length)
	for idx := lengthUnsigned; idx != 0; idx-- {
		shiftbits := (idx - 1) * 8
		mask := uint64(0xFF) << shiftbits
		err := WriteByte(w, byte((iUnsigned&mask)>>shiftbits))
		if err != nil {
			return nn, err
		}
		nn++
	}
	return nn, nil
}

func ReadBoolean(r io.Reader) (bool, error) {
	b, err := ReadByte(r)
	if err != nil {
		return false, err
	}
	return b != 0, nil
}
func WriteBoolean(w io.Writer, v bool) error {
	var b byte
	if v {
		b = 1
	} else {
		b = 0
	}
	return WriteByte(w, b)
}

func ReadInt(r io.Reader) (int32, error) {
	var result int32
	if err := binary.Read(r, binary.BigEndian, &result); err != nil {
		return 0, err
	}
	return result, nil
}
func WriteInt(w io.Writer, v int32) error {
	if err := binary.Write(w, binary.BigEndian, &v); err != nil {
		return err
	}
	return nil
}

func ReadBuffer(r io.Reader) ([]byte, error) {
	size, err := ReadVLong(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
func WriteBuffer(w io.Writer, buf []byte) (int, error) {
	nn, err := WriteVLong(w, int64(len(buf)))
	if err != nil {
		return nn, err
	}
	nn2, err := w.Write(buf)
	if err != nil {
		return nn + nn2, err
	}
	return nn + nn2, nil
}
