package string

import "zeroDB/storage"

// StrData 是 string 数据在内存中的 index 的储存结构
type StrData struct {
	Meta   *storage.Meta //meta info
	Offset int64         // 查询的位置
	FileId uint32        // file id
}
