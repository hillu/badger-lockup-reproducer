# A demonstrator for a BadgerDB deadlock condition

This program demonstrates a hard lockup that happens within
[BadgerDB](https://github.com/dgraph-io/badger). It decodes sparse
bitmaps from 135 Base64 strings and tries to add those to an inverted
index that is stored using BadgerDB. It hangs on inserting the last
entry.

I have reproduced the lockup on Linux/amd64, with Go 1.8.x and 1.9.2,
using BadgerDB version 1.3.0 and the latest commit at the time
(39bfb9cbbe922c50a53aa839476d052b19eaa519).

The symptoms seem similar to what has been described in
BadgerDB issue [#384](https://github.com/dgraph-io/badger/issues/384).

I have submitted this problem as issue
[#410](https://github.com/dgraph-io/badger/issues/410).

 -- Hilko Bengen <bengen@hilluzination.de>

## How to reproduce

- Use `dep` (https://github.com/golang/dep) to populate the vendor
  directory
- Build, using `go build`
- Run `./badger-lockup-reproducer`
