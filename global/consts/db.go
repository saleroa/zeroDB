package consts

import "os"

const (

	// reclaim（修改合并） 的文件
	ReclaimPath = string(os.PathSeparator) + "zerokv_reclaim"

	//extra info 的分隔符，某些命令不能包含
	ExtraSeparator = "\\0"

	// 数据种类的数量，5个
	DataStructureNum = 5

	// 保存zerodb config文件的路径
	ConfigSaveFile = string(os.PathSeparator) + "DB.CFG"

	// 保存transaction meta info文件的路径
	DbTxMetaSaveFile = string(os.PathSeparator) + "DB.TX.META"
)
