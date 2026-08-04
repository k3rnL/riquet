//go:build e2e

package kafka

// SetAfterCommitHookForTest installs a deterministic lost-response boundary.
// It is compiled only into explicit e2e/fault-test builds.
func (s *Store) SetAfterCommitHookForTest(hook func()) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.afterCommit = hook
}
