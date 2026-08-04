package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCSatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("offset out of range : %d", e.Offset),
	)

	msg := fmt.Sprintf(
		"The request offset is outside the logs' range :%d",
		e.Offset,
	)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}

	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCSatus().Err().Error()
}
