package data

// LogRecordPos is a struct that represents the position of data record on the disk.
type LogRecordPos struct {
	Fid    uint32 // File ID : represents that the file in which the data will be stored.
	Offset int64  // offset in the file : represents where the data will be stored in the data file.
}
