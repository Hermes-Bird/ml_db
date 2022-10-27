package data

import "log"

type Stack struct {
	List *ListNode
}

func NewStack() *Stack {
	return &Stack{List: nil}
}

func (s *Stack) Push(v any) {
	log.Println(s.List, s.List == nil)
	if s.List == nil {
		s.List = &ListNode{Val: v, Next: nil}
	} else {
		s.List = &ListNode{Val: v, Next: s.List}
	}
}

func (s *Stack) Pop() any {
	if s.List == nil {
		return nil
	}

	v := s.List.Val
	s.List = s.List.Next

	return v
}
