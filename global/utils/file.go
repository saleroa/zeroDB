package utils

import (
	"io"
	"io/fs"
	"os"
	"path"
)

// 判断路径是否存在
func Exist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	return true
}

// 将 src 的目录复制到 dst
func CopyDir(src string, dst string) error {
	var (
		err     error
		dir     []os.FileInfo
		srcInfo os.FileInfo
	)

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}
	if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}

	if dir, err = ReadDir(src); err != nil {
		return err
	}

	for _, fd := range dir {
		srcPath := path.Join(src, fd.Name())
		dstPath := path.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = CopyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err = CopyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// 将 src 的文件复制到 dst
func CopyFile(src, dst string) error {
	var (
		err     error
		srcFile *os.File
		dstFie  *os.File
		srcInfo os.FileInfo
	)

	if srcFile, err = os.Open(src); err != nil {
		return err
	}

	defer srcFile.Close()

	if dstFie, err = os.Create(dst); err != nil {
		return err
	}

	defer dstFie.Close()

	if _, err = io.Copy(dstFie, srcFile); err != nil {
		return err
	}

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}

	return os.Chmod(dst, srcInfo.Mode())
}

// 从文件夹中读取文件内容
func ReadDir(name string) ([]fs.FileInfo, error) {
	entries, err := os.ReadDir(name)
	if err != nil {
		return nil, err
	}
	infos := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}
