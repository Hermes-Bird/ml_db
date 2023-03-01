package data

type Queue struct {
	List     *ListNode
	LastNode *ListNode
}

func NewQueue() *Queue {
	return &Queue{
		List:     nil,
		LastNode: nil,
	}
}

func (q *Queue) Queue(v interface{}) {
	newNode := &ListNode{
		Val:  v,
		Next: nil,
	}

	if q.List == nil {
		q.List = newNode
		q.LastNode = newNode
	} else {
		q.LastNode = newNode
		q.LastNode.Next = newNode
	}
}

func (q *Queue) Unqueue() interface{} {
	if q.List == nil {
		return nil
	} else {
		v := q.List.Val
		q.List = q.List.Next
		return v
	}
}
