//go:build wlt_recovery_fixture

package mux

// Snapshot exact session identities, without mutating mux state.
func (c *Client) RecoveryFixtureSessions() []any {
	c.access.Lock()
	defer c.access.Unlock()
	result := make([]any, 0)
	for _, session := range c.connections.Array() {
		result = append(result, session)
	}
	return result
}
