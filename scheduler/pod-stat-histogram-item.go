package scheduler

import (
    // "math"
    // "sync"
    // "time"
    // "uta.edu/aces/jade-go/kernel"
)

type HistogramItem struct {
    Value float64
    Left *HistogramItem
    Right *HistogramItem
    Parent *HistogramItem
    Smaller *HistogramItem
    Larger *HistogramItem
    Height int64
    Count int64
}

func NewHistogramItem(v float64) *HistogramItem {
    return &HistogramItem{
            Value: v,
            Left: nil,
            Right: nil,
            Parent: nil,
            Smaller: nil,
            Larger: nil,
            Height: 1,
            Count: 1,
    }
}

func (t *HistogramItem) Insert(v float64) *HistogramItem {
    if v == t.Value {
        t.Count += 1
        return t
    } else if (t.Left == nil && v < t.Value) || ( t.Right == nil && v > t.Value ) {
        newItem := NewHistogramItem(v)
        newItem.Parent = t
        if v > t.Value {
            t.Right = newItem
            newItem.Larger = t.Larger
            newItem.Smaller = t
            t.Larger = newItem
            if newItem.Larger != nil {
                newItem.Larger.Smaller = newItem
            }
        } else {
            t.Left = newItem
            newItem.Smaller = t.Smaller
            newItem.Larger = t
            t.Smaller = newItem
            if newItem.Smaller != nil {
                newItem.Smaller.Larger = newItem
            }
        }
        // update count before rotation
        for p := t; p != nil; p = p.Parent {
            p.Count += 1
        }
        // update height
        if (t.Left == nil && v > t.Value) || (t.Right == nil && v < t.Value) {
            t.Height += 1
            t.UpdateHeight()
        }
        return newItem
    } else if v < t.Value {
        return t.Left.Insert(v)
    } else {
        return t.Right.Insert(v)
    }
}

func (t *HistogramItem) Delete() *HistogramItem {
    if t.Count > 1 {
        t.Count -= 1
        return t
    }

    var tmp *HistogramItem = nil
    if t.Left != nil {
        tmp = t.FindLargestInLeft()
        if tmp.Left != nil {
            tmp.Left.Parent = tmp.Parent
            tmp.Parent.Right = tmp.Left
        } else {
            tmp.Parent.Right = nil
        }

    } else if t.Right != nil {
        tmp = t.FindSmallestInRight()
        if tmp.Right != nil {
            tmp.Right.Parent = tmp.Parent
            tmp.Parent.Left = tmp.Right
        } else {
            tmp.Parent.Left = nil
        }
    }
    if tmp != nil {
        affectedNode := tmp.Parent

        // update stats
        tmp.Count = t.Count
        tmp.Height = t.Height

        // update pointers
        tmp.Parent = t.Parent
        tmp.Left = t.Left
        tmp.Right = t.Right
        tmp.Smaller = t.Smaller
        tmp.Larger = t.Larger

        if t.Left != nil {
            t.Left.Parent = tmp
        }
        if t.Right != nil {
            t.Right.Parent = tmp
        }
        if t.Smaller != nil {
            t.Smaller.Larger = tmp
        }
        if t.Larger != nil {
            t.Larger.Smaller = tmp
        }

        // nil t's pointers
        t.Right = nil
        t.Left = nil
        t.Smaller = nil
        t.Larger = nil

        // Update Count
        for p := affectedNode; p != nil; p = p.Parent {
            p.Count -= 1
        }

        affectedNode.UpdateHeight()
    }
    return tmp
}

func (t *HistogramItem) FindSmallestInRight() *HistogramItem {
    if t.Right == nil {
        return nil
    } else {
        c := t.Right
        for ; c.Left != nil;  c = c.Left {
        }
        return c
    }
}

func (t *HistogramItem) FindLargestInLeft() *HistogramItem {
    if t.Left == nil {
        return nil
    } else {
        c := t.Left
        for ; c.Right != nil;  c = c.Right {
        }
        return c
    }
}

func (t *HistogramItem) CalcHeight() (int64, int64, int64) {
    leftHeight := int64(0)
    rightHeight := int64(0)
    if t.Left != nil {
        leftHeight = t.Left.Height 
    }
    if t.Right != nil {
        rightHeight = t.Right.Height
    }

    t.Height = leftHeight + 1
    if rightHeight > leftHeight {
        t.Height = rightHeight + 1
    }
    return t.Height, leftHeight, rightHeight
}

func (t *HistogramItem) UpdateHeight() {
    for c := t; c != nil; c = c.Parent {
        _, leftHeight, rightHeight := c.CalcHeight()

        if leftHeight - rightHeight > 1 {
            if c.Left.Right != nil {
                c.Left.LeftRotate()
            }
            c = c.RightRotate()
        } else if rightHeight - leftHeight > 1 {
            if c.Right.Left != nil {
                c.Right.RightRotate()
            }
            c = c.LeftRotate()
        }
    }
}

func (t *HistogramItem) LeftRotate() *HistogramItem{
    if t.Right == nil {
        return t
    }

    p := t.Right

    t.Right = p.Left
    if p.Left != nil {
        p.Left.Parent = t
    }

    t.Parent = p
    p.Left = t

    t.CalcHeight()
    p.CalcHeight()

    return p
}

func (t *HistogramItem) RightRotate() *HistogramItem{
    if t.Left == nil {
        return t
    }

    p := t.Left

    t.Left = p.Right
    if p.Right != nil {
        p.Right.Parent = t
    }

    t.Parent = p
    p.Right = t

    t.CalcHeight()
    p.CalcHeight()

    return p
}
