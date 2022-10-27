package json_handler

type ComparableJSON interface {
	Matches(data []byte) bool
}
