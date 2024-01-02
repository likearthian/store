package store

func sliceMap[In any, Out any](list []In, mapFn func(val In) Out) []Out {
	var newSlice = make([]Out, len(list))
	for i, val := range list {
		newSlice[i] = mapFn(val)
	}

	return newSlice
}

func sliceContains[T comparable](list []T, val T) bool {
	for _, item := range list {
		if item == val {
			return true
		}
	}

	return false
}

func sliceFilter[T any](slice []T, filterFunc func(val T) bool) []T {
	var newSlice []T
	for i, val := range slice {
		if filterFunc(val) {
			newSlice = append(newSlice, slice[i])
		}
	}

	return newSlice
}

func SplitBatch[T any](list []T, chunk int) [][]T {
	total := len(list)
	rem := total % chunk
	batch := total / chunk

	if rem > 0 {
		batch++
	}

	var newList = make([][]T, batch)
	start := 0
	end := chunk
	for i := 0; i < batch; i++ {
		if end > total {
			end = total
		}

		newList[i] = list[start:end]
		start += chunk
		end += chunk
	}

	return newList
}
