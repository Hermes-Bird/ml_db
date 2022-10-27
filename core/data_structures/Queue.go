package data

type Queue struct {
	List *ListNode
}

func NewQueue() *Queue {
	return &Queue{
		List: nil,
	}
}

func (q Queue) Queue(v interface{}) {
	newNode := &ListNode{
		Val:  v,
		Next: nil,
	}
	if q.List == nil {
		q.List = newNode
	} else {
		newNode.Next = q.List
		q.List = newNode
	}
}

func (q Queue) Unqueue() interface{} {
	if q.List == nil {
		return nil
	} else {
		v := q.List.Val
		q.List = q.List.Next
		return v
	}
}
