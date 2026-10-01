package handler

// validator collects field errors so that one response can report every problem
// with a request instead of the first one found.
type validator struct {
	Errors map[string]string
}

func newValidator() *validator {
	return &validator{Errors: make(map[string]string)}
}

func (v *validator) Valid() bool {
	return len(v.Errors) == 0
}

func (v *validator) AddError(key, message string) {
	if _, exists := v.Errors[key]; !exists {
		v.Errors[key] = message
	}
}

func (v *validator) Check(ok bool, key, message string) {
	if !ok {
		v.AddError(key, message)
	}
}
