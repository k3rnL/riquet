package domain

import (
	"errors"
	"fmt"
)

// ErrorCategory is stable across HTTP and storage adapters.
type ErrorCategory string

const (
	// ErrorInvalid identifies invalid client input.
	ErrorInvalid ErrorCategory = "invalid"
	// ErrorNotFound identifies an absent registry resource.
	ErrorNotFound ErrorCategory = "not_found"
	// ErrorConflict identifies state that prevents a requested mutation.
	ErrorConflict ErrorCategory = "conflict"
	// ErrorIncompatible identifies a rejected schema evolution.
	ErrorIncompatible ErrorCategory = "incompatible"
	// ErrorReadOnly identifies a mutation blocked by registry mode.
	ErrorReadOnly ErrorCategory = "read_only"
	// ErrorStorage identifies a failed durable commit.
	ErrorStorage ErrorCategory = "storage"
	// ErrorCorrupt identifies invalid persisted state or transitions.
	ErrorCorrupt ErrorCategory = "corrupt"
)

// Error is an endpoint-independent domain failure.
type Error struct {
	Category ErrorCategory
	Resource string
	Detail   string
	Cause    error
}

func (e *Error) Error() string {
	message := string(e.Category)
	if e.Resource != "" {
		message += " " + e.Resource
	}
	if e.Detail != "" {
		message += ": " + e.Detail
	}
	return message
}

func (e *Error) Unwrap() error { return e.Cause }

func domainError(category ErrorCategory, resource, format string, args ...any) error {
	return &Error{Category: category, Resource: resource, Detail: fmt.Sprintf(format, args...)}
}

// CategoryOf returns the stable category of a domain error.
func CategoryOf(err error) ErrorCategory {
	var typed *Error
	if errors.As(err, &typed) {
		return typed.Category
	}
	return ""
}
