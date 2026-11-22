package http3conn

// handlerErrorMap changes only the error exposed to a handler. Protocol state
// and cleanup retain their original errors. A nil map is the internal default.
type handlerErrorMap func(error) error

func (m handlerErrorMap) apply(err error) error {
	if err == nil || m == nil {
		return err
	}
	return m(err)
}
