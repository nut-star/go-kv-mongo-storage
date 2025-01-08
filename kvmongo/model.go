package kvmongo

import "strings"

type item[K comparable, V any] struct {
	id      K     `bson:"_id"`
	version int64 `bson:"version"`
	value   V     `bson:"value"`
}

const (
	NotFoundErr       string = "NOT_FOUND_ERROR"
	DBErr             string = "DB_ERROR"
	DecodeErr         string = "DECODE_ERROR"
	NotImplementedErr string = "NOT_IMPLEMENTED_YET"
	OptimisticLockErr string = "OPTIMISTIC_LOCK"
)

type kvErrorInfo struct {
	code   string
	reason error
}

type KVError interface {
	Code() string
	Reason() error
}

func parseKVError(err error) *kvErrorInfo {
	if err.Error() == "mongo: no documents in result" {
		return &kvErrorInfo{NotFoundErr, err}
	}
	if strings.Contains(err.Error(), "E11000") {
		return &kvErrorInfo{OptimisticLockErr, err}
	}
	return &kvErrorInfo{DBErr, err}
}

func newKVError(code string, err error) *kvErrorInfo {
	return &kvErrorInfo{code, err}
}

func (e *kvErrorInfo) Code() string {
	return e.code
}

func (e *kvErrorInfo) Reason() error {
	return e.reason
}
