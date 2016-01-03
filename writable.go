package hadoop

import "io"
import "encoding/binary"

type Writable interface {
	Write(w io.Writer) (int, error)
	Read(r io.Reader) error
}

type IntWritable int32

func (self *IntWritable) Write(w io.Writer) (int, error) {
	err := binary.Write(w, binary.BigEndian, self)
	if err != nil {
		return 0, err
	}
	return 4, nil
}

func (self *IntWritable) Read(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, self)
}

type LongWritable int64

func (self *LongWritable) Write(w io.Writer) (int, error) {
	err := binary.Write(w, binary.BigEndian, self)
	if err != nil {
		return 0, err
	}
	return 8, nil
}

func (self *LongWritable) Read(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, self)
}

type TextWritable struct {
	Buf []byte
}

func (self *TextWritable) Write(w io.Writer) (int, error) {
	nn1, err := WriteVLong(w, int64(len(self.Buf)))
	if err != nil {
		return 0, err
	}
	nn2, err := w.Write(self.Buf)
	return nn1 + nn2, err
}

func (self *TextWritable) Read(r io.Reader) error {
	size, err := ReadVLong(r)
	if err != nil {
		return err
	}

	if self.Buf == nil || cap(self.Buf) < int(size) {
		self.Buf = make([]byte, size)
	}
	self.Buf = self.Buf[0:size]
	if _, err := io.ReadFull(r, self.Buf); err != nil {
		return err
	}
	return nil
}

type BytesWritable struct {
	Buf []byte
}

func (self *BytesWritable) Write(w io.Writer) (int, error) {
	if self.Buf == nil {
		err := binary.Write(w, binary.BigEndian, int32(0))
		if err != nil {
			return 0, err
		}
		return 4, nil
	} else {
		if err := binary.Write(w, binary.BigEndian, int32(len(self.Buf))); err != nil {
			return 0, err
		}
		nn, err := w.Write(self.Buf)
		return nn + 4, err
	}
}

func (self *BytesWritable) Read(r io.Reader) error {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return err
	}
	if self.Buf == nil || cap(self.Buf) < int(size) {
		self.Buf = make([]byte, size)
	}
	self.Buf = self.Buf[0:size]
	if _, err := io.ReadFull(r, self.Buf); err != nil {
		return err
	}
	return nil
}
