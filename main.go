package main

import (
	"github.com/RoaringBitmap/roaring"

	"github.com/dgraph-io/badger"

	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func encodeKey(t uint32) (buf []byte) {
	buf = []byte("i/....")
	binary.LittleEndian.PutUint32(buf[2:], uint32(t))
	return
}

func decodeKey(buf []byte) (uint32, error) {
	if !bytes.HasPrefix(buf, []byte("i/")) {
		return 0, errors.New("wrong prefix")
	}
	buf = buf[2:]
	if len(buf) != 4 {
		return 0, errors.New("wrong length")
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func getBitmap(txn *badger.Txn, key []byte) (bitmap *roaring.Bitmap, err error) {
	bitmap = roaring.New()
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return bitmap, nil
	} else if err != nil {
		return nil, err
	}
	value, err := item.Value()
	if err != nil {
		return nil, err
	}
	if err := bitmap.UnmarshalBinary(value); err != nil {
		return nil, fmt.Errorf("could not read entry %08x", key, err)
	}
	return
}

func putBitmap(txn *badger.Txn, key []byte, bitmap *roaring.Bitmap) (uint64, error) {
	if bitmap.IsEmpty() {
		return 0, txn.Delete(key)
	}
	if value, err := bitmap.MarshalBinary(); err != nil {
		return 0, err
	} else {
		// log.Printf("Writing %d bytes to key %q...", len(value), key)
		return uint64(len(value)), txn.Set(key, value)
	}
}

var db *badger.DB

func insert(entries *roaring.Bitmap, filtered bool) (id uint32, err error) {
	var n, size uint64
	for {
		err = db.Update(func(txn *badger.Txn) (err error) {
			var blobIDs *roaring.Bitmap
			if blobIDs, err = getBitmap(txn, []byte("m/blobs")); err != nil {
				return
			}
			if !blobIDs.IsEmpty() {
				id = blobIDs.Maximum() + 1
			}
			blobIDs.Add(id)
			if n, err = putBitmap(txn, []byte("m/blobs"), blobIDs); err != nil {
				return
			}
			size += n
			if filtered {
				var partialIDs *roaring.Bitmap
				if partialIDs, err = getBitmap(txn, []byte("m/filtered-blobs")); err != nil {
					return
				}
				partialIDs.Add(uint32(id))
				if n, err = putBitmap(txn, []byte("m/filtered-blobs"), partialIDs); err != nil {
					return
				}
				size += n
			}
			return
		})
		if err == nil {
			break
		}
	}

newTxn:
	for todo, done := entries.Clone(), roaring.New(); !todo.IsEmpty(); {
		txn := db.NewTransaction(true)
		it := todo.Iterator()
		for it.HasNext() {
			t := it.Next()
			key := encodeKey(t)
			var entry *roaring.Bitmap
			if entry, err = getBitmap(txn, key); err != nil {
				txn.Discard()
				return 0, fmt.Errorf("could not read entry %08x", t, err)
			}
			entry.Add(uint32(id))
			if n, err = putBitmap(txn, key, entry); err == badger.ErrTxnTooBig {
				break
			} else if err != nil {
				txn.Discard()
				return
			}
			size += n
			done.Add(uint32(t))
		}
		if err = txn.Commit(nil); err == badger.ErrConflict {
			txn.Discard()
			goto newTxn
		}
		todo.AndNot(done)
	}
	log.Printf("Wrote %d bytes to values", size)
	return
}

func main() {
	var path string
	var err error
	if len(os.Args) >= 2 {
		path = os.Args[1]
	} else {
		path, err = ioutil.TempDir(".", "db-")
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("Using db directory %s", path)
	options := badger.DefaultOptions
	options.Dir = path
	options.ValueDir = path
	if db, err = badger.Open(options); err != nil {
		log.Fatal(err)
	}
	bm := roaring.New()
	for i, entry := range testdata {
		buf, err := base64.StdEncoding.DecodeString(entry.b64)
		if err != nil {
			log.Fatal(err)
		}
		rd := bytes.NewReader(buf)
		bm.Clear()
		if _, err := bm.ReadFrom(rd); err != nil {
			log.Fatal(err)
		}
		log.Printf("entry %d, cardinality: %d", i, bm.GetCardinality())
		if _, err = insert(bm, entry.filtered); err != nil {
			log.Fatal(err)
		}
	}
}
