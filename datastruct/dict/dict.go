package dict

// Consumer is used to traversal dict, if it returns false the traversal will be break 用于遍历dict，如果返回false，则遍历将被中断
type Consumer func(key string, val interface{}) bool

// Dict is interface of a key-value data structure
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	//PutIfAbsent(key string, val interface{}) (result int)
	//PutIfExists(key string, val interface{}) (result int)
	//Remove(key string) (val interface{}, result int)
	// ForEach(consumer Consumer)
	//Keys() []string
	//RandomKeys(limit int) []string
	//RandomDistinctKeys(limit int) []string
	//Clear()
	//DictScan(cursor int, count int, pattern string) ([][]byte, int)
}
