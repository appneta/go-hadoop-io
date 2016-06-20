package hadoop

// #cgo CFLAGS: -O3
import "C"

import (
	"bytes"
	"compress/bzip2"
	"compress/zlib"
	"io"
)

var (
	Codecs map[string]Codec = map[string]Codec{
		"org.apache.hadoop.io.compress.DefaultCodec": &ZlibCodec{},
		"org.apache.hadoop.io.compress.Lz4Codec":     &Lz4Codec{},
		"org.apache.hadoop.io.compress.BZip2Codec":   &Bzip2Codec{},
	}
)

type Codec interface {
	Uncompress(dst, src []byte) ([]byte, error)
	Compress(dst, src []byte) ([]byte, error)
}

type ZlibCodec struct{}

func (c *ZlibCodec) Uncompress(dst, src []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	var buf [512]byte
	for {
		n, err := reader.Read(buf[:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		dst = append(dst, buf[:n]...)
		if err == io.EOF {
			break
		}
	}
	return dst, nil
}

func (c *ZlibCodec) Compress(dst, src []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	_, err := writer.Write(src)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type Bzip2Codec struct {
}

func (c *Bzip2Codec) Compress(dst, src []byte) ([]byte, error) {
	panic("Bzip2Codec Compress not implemented")
}

func (c *Bzip2Codec) Uncompress(dst, src []byte) ([]byte, error) {
	reader := bzip2.NewReader(bytes.NewReader(src))
	var buf [512]byte
	for {
		n, err := reader.Read(buf[:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		dst = append(dst, buf[:n]...)
		if err == io.EOF {
			break
		}
	}
	return dst, nil
}

type Lz4Codec struct {
}

func (c *Lz4Codec) Compress(dst, src []byte) ([]byte, error) {
	panic("Lz4Codec Compress not implemented")
}

// func lz4DecompressSafe(in, out []byte) (int, error) {
// 	n := int(C.LZ4_decompress_safe((*C.char)(unsafe.Pointer(&in[0])), (*C.char)(unsafe.Pointer(&out[0])), C.int(len(in)), C.int(len(out))))
// 	if n < 0 {
// 		return 0, errors.New("corrupt input")
// 	}
// 	return n, nil
// }

func (c *Lz4Codec) Uncompress(dst, src []byte) ([]byte, error) {
	panic("Lz4Codec Uncompress has been disabled / commented out")
	// var iptr, optr uint

	// osize := uint(binary.BigEndian.Uint32(src[0:]))
	// iptr += 4
	// out := make([]byte, osize)

	// for optr < osize {
	// 	iblocksize := uint(binary.BigEndian.Uint32(src[iptr:]))
	// 	iptr += 4

	// 	n, err := lz4DecompressSafe(src[iptr:iptr+iblocksize], out[optr:])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	optr += uint(n)
	// 	iptr += iblocksize
	// }

	// return out, nil
}

type SnappyCodec struct {
}
