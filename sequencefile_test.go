package hadoop

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSequenceFile(t *testing.T) {

	fp, err := os.Open("test-bc-long-bytes.seq.bz2")
	if err != nil {
		panic(err)
	}
	defer fp.Close()
	reader, err := NewSequenceFileReader(fp)
	if err != nil {
		panic(err)
	}

	var key LongWritable
	var value BytesWritable

	for {
		if err := reader.Read(&key, &value); err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		fmt.Println(key, ": ", value)
	}

	reader.Close()

	// fmt.Println("test")
	// hio.IntWritable(10).Write(os.Stdout)

	// var value hio.IntWritable
	// value.Read(os.Stdin)
	// value.Write(os.Stdout)
	// fmt.Println(value)

	// bytes := &hio.BytesWritable{[]byte("string")}
	// bytes.Write(os.Stdout)

	// var r hio.BytesWritable
	// if err := r.Read(os.Stdin); err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(string(r.Buf))
}

func genTestData(seed int) (string, string) {
	gen := rand.New(rand.NewSource(int64(seed)))
	genByte := func() byte {
		return byte(gen.Intn(256))
	}
	genString := func(length int) string {
		buf := []byte{}
		for i := 0; i < length; i++ {
			buf = append(buf, genByte())
		}
		return string(buf)
	}
	keySize := gen.Intn(2000)
	valueSize := gen.Intn(2000)
	// Include a few large cases:
	if seed < 2 {
		keySize = 1000000 + gen.Intn(1000000)
		valueSize = 1000000 + gen.Intn(1000000)
	}
	return genString(keySize), genString(valueSize)
}

// Write a sequence file filled with random data, then read it back and assert that the values read
// match the values written.
func TestWriteThenRead(t *testing.T) {
	assert := assert.New(t)
	NUM_RECORDS := 100

	var key TextWritable
	var value BytesWritable

	writerOpts := &SequenceFileWriterOpts{}
	buf := bytes.Buffer{}
	writer, err := NewSequenceFileWriter(&buf, writerOpts)
	assert.NoError(err)
	for i := 0; i < NUM_RECORDS; i++ {
		keyStr, valueStr := genTestData(i)
		key.Buf = []byte(keyStr)
		value.Buf = []byte(valueStr)
		err := writer.Write(&key, &value)
		assert.NoError(err)
	}
	err = writer.Close()
	assert.NoError(err)

	// log.Printf("Sequence file is %d bytes long\n", buf.Len())

	reader, err := NewSequenceFileReader(&buf)
	assert.NoError(err)
	for i := 0; i < NUM_RECORDS+1; i++ {
		err := reader.Read(&key, &value)
		if i == NUM_RECORDS {
			assert.Equal(err, io.EOF)
			break
		}
		assert.NoError(err)
		if err != nil {
			break
		}
		keyStr, valueStr := genTestData(i)
		assert.Equal(keyStr, string(key.Buf))
		assert.Equal(valueStr, string(value.Buf))
	}
	err = reader.Close()
	assert.NoError(err)
}
