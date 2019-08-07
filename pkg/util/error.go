package util

import "errors"

var (
	ErrInvalidBrokerID             = errors.New("invalid broker id")
	ErrUnavailableBroker           = errors.New("unavailable broker")
	ErrControllerSelectionFatal    = errors.New("controller selection fatal")
	ErrControllerSelectionConflict = errors.New("controller selection conflict")
	ErrControllerOutOfTerm         = errors.New("controller is out of term")
)
