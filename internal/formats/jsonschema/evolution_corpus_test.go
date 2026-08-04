package jsonschema

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
)

type evolutionCase struct {
	Name       string                    `json:"name"`
	Level      domain.CompatibilityLevel `json:"level"`
	Previous   []string                  `json:"previous"`
	Candidate  string                    `json:"candidate"`
	Compatible bool                      `json:"compatible"`
}

func TestEvolutionCorpus(t *testing.T) {
	raw, err := os.ReadFile("testdata/evolution.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []evolutionCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	engine := Engine{}
	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			parse := func(definition string) formats.Parsed {
				parsed, parseErr := engine.Parse(context.Background(), formats.ParseRequest{Definition: definition}, nil)
				if parseErr != nil {
					t.Fatal(parseErr)
				}
				return parsed
			}
			candidate := parse(testCase.Candidate)
			previous := make([]formats.Parsed, 0, len(testCase.Previous))
			for _, definition := range testCase.Previous {
				previous = append(previous, parse(definition))
			}
			compatible, messages, compatibleErr := engine.Compatible(context.Background(), testCase.Level, candidate, previous)
			if compatibleErr != nil {
				t.Fatal(compatibleErr)
			}
			if compatible != testCase.Compatible {
				t.Fatalf("compatible = %v, want %v: %v", compatible, testCase.Compatible, messages)
			}
		})
	}
}
