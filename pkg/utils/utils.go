package utils

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

// GetHashFromHeader returns "" when digest not like “SHA-256=<hash>”
func GetHashFromHeader(h http.Header) string {
	digest := h.Get("digest")
	log.Println("hash: ", digest)
	if len(digest) < 9 {
		return ""
	}
	if digest[:8] != "SHA-256=" {
		return ""
	}
	return digest[8:]
}

func GetSizeFromHeader(h http.Header) int64 {
	size, _ := strconv.ParseInt(h.Get("Content-Length"), 0, 64)
	return size
}

func CalculateHash(reader io.Reader) string {
	h := sha256.New()
	io.Copy(h, reader)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func GetOffsetFromHeader(h http.Header) int64 {
	byteRange := h.Get("range")
	if len(byteRange) < 7 {
		return 0
	}
	if byteRange[:6] != "bytes=" {
		return 0
	}
	bytePos := strings.Split(byteRange[6:], "-")
	offset, _ := strconv.ParseInt(bytePos[0], 0, 64)
	return offset
}
