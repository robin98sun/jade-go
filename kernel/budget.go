package kernel

// Budget the budget specification
type Budget struct {
	MaximumMilliseconds int `json:"maximumMilliseconds,omitempty"`
	Price               int `json:"price,omitempty"`
}

func (b *Budget) valid() bool {
	if b.MaximumMilliseconds == 0 || b.Price == 0 {
		return false
	}
	return true
}

func (b *Budget) Copy() *Budget {
	newBudget := &Budget{
		MaximumMilliseconds: b.MaximumMilliseconds,
		Price:               b.Price,
	}
	return newBudget
}
