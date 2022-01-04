package redis_client

import "fmt"

type Result struct {
	content interface{}
}

func (r *Result) Err() error {
	err, ok := r.content.(error)
	if !ok {
		return nil
	}

	return err
}

func (r *Result) Int64() (int64, error) {
	if err := r.Err(); err != nil {
		return 0, err
	}

	result, ok := r.content.(int64)
	if !ok {
		return 0, fmt.Errorf("content is not an int64: %#v", r.content)
	}

	return result, nil
}

func (r *Result) String() (string, bool, error) {
	if err := r.Err(); err != nil {
		return "", false, err
	}

	if r.content == nil {
		return "", true, nil
	}

	result, ok := r.content.(string)
	if !ok {
		return "", false, fmt.Errorf("content is not an string: %#v", r.content)
	}

	return result, false, nil
}

func (r *Result) Slice() ([]interface{}, error) {
	if err := r.Err(); err != nil {
		return nil, err
	}

	if r.content == nil {
		return nil, nil
	}

	result, ok := r.content.([]interface{})
	if !ok {
		return nil, fmt.Errorf("content is not a slice: %#v", r.content)
	}

	return result, nil
}

func (r *Result) Content() interface{} {
	return r.content
}
