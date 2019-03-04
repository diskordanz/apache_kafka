package main

import (
	"testing"
)

func TestIsPrime(t *testing.T) {

	var tt = []struct {
		input  int
		result bool
	}{
		{
			input:  3,
			result: true,
		},
		{
			input:  6,
			result: false,
		},
		{
			input:  12,
			result: false,
		},
		{
			input:  13,
			result: true,
		},
	}

	for _, tc := range tt {

		if isPrime(tc.input) != tc.result {
			t.Errorf("%d должно быть %t, а получилось %t\n\n", tc.input, tc.result, isPrime(tc.input))
		}

	}

}

func TestFactorize(t *testing.T) {

	var tt = []struct {
		input  int
		result bool
	}{
		{
			input: 1,
		},
		{
			input: 2,
		},
		{
			input: 3,
		},
		{
			input: 5,
		},
		{
			input: 80,
		},
		{
			input: 167777,
		},
		{
			input: 10000000,
		},
	}

	ch := make(chan []int)
	var factors []int

	for _, tc := range tt {

		go factorize(tc.input, ch)
		factors = <-ch
		t.Logf("\n%+v\n\n", factors)

	}

}

func BenchmarkFactorize(b *testing.B) {
	ch := make(chan []int)
	var factors []int
	for i := 1; i < b.N; i++ {
		num := 2345678901234
		go factorize(num, ch)
		factors = <-ch
		b.Logf("\n%d раскладывется на %+v\n\n", num, factors)
	}

}