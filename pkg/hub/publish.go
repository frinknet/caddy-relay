package hub

// Result is the return of Hub.Publish. It reports, per channel name, whether
// the message was enqueued. Its String() is empty so bare use in a template
// emits nothing — use range or index to inspect.
type Result map[string]bool

// String returns "" so a bare {{ ... | publish ... }} emits nothing.
func (Result) String() string { return "" }

// Publish fan-out is delegated to Hub.Publish (hub.go); this file exists to
// keep the Result type and its Stringer together and to document the
// per-channel keying contract.