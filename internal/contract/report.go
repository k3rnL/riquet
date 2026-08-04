package contract

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Report is a minimal reproducible differential result.
type Report struct {
	Scenario          string      `json:"scenario"`
	Reference         string      `json:"reference"`
	Candidate         string      `json:"candidate"`
	ReferenceVersion  string      `json:"referenceVersion"`
	Manifest          string      `json:"manifest"`
	CreatedAt         time.Time   `json:"createdAt"`
	Difference        *Difference `json:"difference,omitempty"`
	ReferenceExchange *Exchange   `json:"referenceExchange,omitempty"`
	CandidateExchange *Exchange   `json:"candidateExchange,omitempty"`
}

// NewReport creates a report and includes only the mismatching exchange.
func NewReport(scenario, reference, candidate, version, manifest string, difference *Difference, left, right Trace) Report {
	report := Report{
		Scenario: scenario, Reference: reference, Candidate: candidate,
		ReferenceVersion: version, Manifest: manifest, CreatedAt: time.Now().UTC(), Difference: difference,
	}
	if difference != nil && difference.Exchange >= 0 && difference.Exchange < len(left.Exchanges) {
		report.ReferenceExchange = &left.Exchanges[difference.Exchange]
	}
	if difference != nil && difference.Exchange >= 0 && difference.Exchange < len(right.Exchanges) {
		report.CandidateExchange = &right.Exchanges[difference.Exchange]
	}
	return report
}

// WriteReport writes indented JSON atomically enough for test artifact use.
func WriteReport(filename string, report Report) error {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode differential report: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0o750); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	if err := os.WriteFile(filename, append(encoded, '\n'), 0o600); err != nil {
		return fmt.Errorf("write differential report: %w", err)
	}
	return nil
}
