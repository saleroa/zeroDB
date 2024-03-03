package utils

import "strconv"

// float64 转化成 string
func Float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

// string 转化成 float64
func StrToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}
