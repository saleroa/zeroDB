package storage

import (
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"strings"

	"zeroDB/global/dberror"
	"zeroDB/global/utils"
)

const (
	// 文件权限
	FilePerPm     = 0644
	PathSeparator = "/"
)

var (
	// 数据文件的命名格式
	// 09d ,d 表示以10进制输出,09 表示宽度为 9
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

// 储存文件
type DBFile struct {
	Id     uint32
	Path   string   //文件路径
	File   *os.File //数据储存文件
	Offset int64    //文件大小
	// FistMerge      bool     //是否第一次merge过
	// LastMergeSize  int      //上次merge的文件大小
	// FirstMergeSize int
	// MergePercent   float32
}

// 打开一个 dbfile 文件，如果不存在就创建
func NewDBFile(path string, fileId uint32, typ uint16) (*DBFile, error) {
	filepath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[typ])
	//os.O_CREATE|os.O_RDWR
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, FilePerPm)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	df := &DBFile{
		Id:     fileId,
		Path:   path,
		Offset: stat.Size(),
		// MergePercent:   int(cfg.MergePercent),
		// FirstMergeSize: cfg.FirstMergeSize,
	}
	df.File = file
	return df, nil
}

// 读取dbfile中数据文件file，返回的是 encode 后的 []byte ,n 代表读取的数据长度
func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.File.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// 将encode的entry读取出来并且decode
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte
	// 读取 entryhead
	if buf, err = df.readBuf(offset, int64(EntryHeaderSize)); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}

	// 读取 key
	offset += EntryHeaderSize
	var key []byte
	if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
		return
	}
	e.Meta.Key = key

	// 读取 value ，如果存在
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var value []byte
		if value, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = value
	}
	// 读取 es ，如果存在
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var extra []byte
		if extra, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = extra
	}

	// 进行 crc 对比校验，看是否出错
	check := crc32.ChecksumIEEE(e.Meta.Value)
	if e.Crc32 != check {
		return nil, dberror.ErrInvalidCrc
	}
	return
}

// 向 dbfile 中追加写入 entry
func (df *DBFile) Write(e *Entry) error {
	// key 为空 或者 空entry 无法写入
	if e == nil || e.Meta.KeySize == 0 {
		return dberror.ErrEmptyEntry
	}
	writeOffset := df.Offset
	enVal, err := e.Encode()
	if err != nil {
		return err
	}

	if _, err := df.File.WriteAt(enVal, writeOffset); err != nil {
		return err
	}

	//追加内容后，文件变大，offset增加
	df.Offset += int64(e.Size())
	return nil
}

// 立刻将文件保存到硬盘中
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		//将文件系统的最近写入的数据在内存中的拷贝刷新到硬盘中
		err = df.File.Sync()
	}
	return err
}

// 关闭文件，sync表示是否要立刻写入磁盘
func (df *DBFile) Close(sync bool) (err error) {
	if sync {
		if err = df.Sync(); err != nil {
			return
		}
	}
	err = df.File.Close()
	return
}

// 加载磁盘中所有dbfile，返回数据类型和dbfile的map
func Build(path string, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := utils.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			// find the different types of file.
			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	// load all the db files.
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs)
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0

		if len(fileIDs) > 0 {
			activeFileId = uint32(fileIDs[len(fileIDs)-1])

			for i := 0; i < len(fileIDs)-1; i++ {
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id), dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}
	return archFiles, activeFileIds, nil
}
