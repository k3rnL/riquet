package kafka

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/k3rnL/riquet/internal/domain"
)

const (
	stateFormatVersion = 1
	topicPartition     = 0
	transitionPrefix   = "v1/transition/"
	snapshotRecordKey  = "v1/snapshot"
	primaryRecordKey   = "v1/coordination/primary"
	restoreRecordKey   = "v1/restore/checkpoint"
)

func transitionKey(sequence domain.Sequence) string {
	return fmt.Sprintf("%s%020d", transitionPrefix, sequence)
}

func parseTransitionKey(key string) (domain.Sequence, bool) {
	if !strings.HasPrefix(key, transitionPrefix) {
		return 0, false
	}
	raw := strings.TrimPrefix(key, transitionPrefix)
	if len(raw) != 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	return domain.Sequence(value), err == nil && value > 0
}
