package statestore

import (
	"fmt"
)

const (
	ERROR_Runtime = iota
	ERROR_BadArguments
	ERROR_PathNotExist
)

type Error struct {
	code    int
	message string
}

func (e *Error) GetCode() int {
	return e.code
}

func (e *Error) Error() string {
	switch e.code {
	case ERROR_Runtime:
		return fmt.Sprintf("RuntimeError: %s", e.message)
	case ERROR_BadArguments:
		return fmt.Sprintf("BadArguments: %s", e.message)
	case ERROR_PathNotExist:
		return fmt.Sprintf("Path %s not exists", e.message)
	default:
		panic("Unknown error type")
	}
}

func newRuntimeError(message string) error {
	return &Error{code: ERROR_Runtime, message: message}
}

func newBadArgumentsError(message string) error {
	return &Error{code: ERROR_BadArguments, message: message}
}

func newPathNotExistError(path string) error {
	return &Error{code: ERROR_PathNotExist, message: path}
}
