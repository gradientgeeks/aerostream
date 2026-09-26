package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Role string

const (
	RoleSuperAdmin Role = "SUPER_ADMIN"
	RoleOperator   Role = "OPERATOR"
	RoleProducer   Role = "PRODUCER"
	RoleConsumer   Role = "CONSUMER"
	RoleAuditor    Role = "AUDITOR"
)

type ResourceType string

const (
	ResourceTypeTopic   ResourceType = "TOPIC"
	ResourceTypeGroup   ResourceType = "GROUP"
	ResourceTypeCluster ResourceType = "CLUSTER"
)

type Operation string

const (
	OperationRead     Operation = "READ"
	OperationWrite    Operation = "WRITE"
	OperationDescribe Operation = "DESCRIBE"
	OperationAlter    Operation = "ALTER"
	OperationAll      Operation = "ALL"
)

type Permission string

const (
	PermissionAllow Permission = "ALLOW"
	PermissionDeny  Permission = "DENY"
)

type User struct {
	Username  string    `json:"username"`
	Role      Role      `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

type AclRule struct {
	ID           string       `json:"id"`
	Principal    string       `json:"principal"`     // e.g. "User:alice", "User:orders_service", "User:*"
	ResourceType ResourceType `json:"resource_type"` // "TOPIC", "GROUP", "CLUSTER"
	ResourceName string       `json:"resource_name"` // "orders-*", "telemetry", "*"
	Operation    Operation    `json:"operation"`     // "READ", "WRITE", "DESCRIBE", "ALTER", "ALL"
	Permission   Permission   `json:"permission"`    // "ALLOW", "DENY"
	CreatedAt    time.Time    `json:"created_at"`
}

type AclManager struct {
	mu    sync.RWMutex
	rules map[string]*AclRule
	users map[string]*User
}

func NewAclManager() *AclManager {
	mgr := &AclManager{
		rules: make(map[string]*AclRule),
		users: make(map[string]*User),
	}
	mgr.seedDefaults()
	return mgr
}

func (m *AclManager) seedDefaults() {
	now := time.Now().UTC()

	// 1. Seed Users
	defaultUsers := []*User{
		{
			Username:  "admin",
			Role:      RoleSuperAdmin,
			CreatedAt: now,
		},
		{
			Username:  "order_producer",
			Role:      RoleProducer,
			CreatedAt: now,
		},
		{
			Username:  "billing_consumer",
			Role:      RoleConsumer,
			CreatedAt: now,
		},
		{
			Username:  "auditor",
			Role:      RoleAuditor,
			CreatedAt: now,
		},
	}
	for _, u := range defaultUsers {
		m.users[u.Username] = u
	}

	// 2. Seed Default ACL Rules matching standard enterprise zero-trust patterns
	defaultRules := []*AclRule{
		{
			ID:           "acl-order-producer-write-orders",
			Principal:    "User:order_producer",
			ResourceType: ResourceTypeTopic,
			ResourceName: "orders-*",
			Operation:    OperationWrite,
			Permission:   PermissionAllow,
			CreatedAt:    now,
		},
		{
			ID:           "acl-billing-consumer-read-orders",
			Principal:    "User:billing_consumer",
			ResourceType: ResourceTypeTopic,
			ResourceName: "orders-*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
			CreatedAt:    now,
		},
		{
			ID:           "acl-billing-consumer-group",
			Principal:    "User:billing_consumer",
			ResourceType: ResourceTypeGroup,
			ResourceName: "billing-group",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
			CreatedAt:    now,
		},
		{
			ID:           "acl-auditor-describe-topics",
			Principal:    "User:auditor",
			ResourceType: ResourceTypeTopic,
			ResourceName: "*",
			Operation:    OperationDescribe,
			Permission:   PermissionAllow,
			CreatedAt:    now,
		},
		{
			ID:           "acl-auditor-describe-groups",
			Principal:    "User:auditor",
			ResourceType: ResourceTypeGroup,
			ResourceName: "*",
			Operation:    OperationDescribe,
			Permission:   PermissionAllow,
			CreatedAt:    now,
		},
		{
			ID:           "acl-auditor-describe-cluster",
			Principal:    "User:auditor",
			ResourceType: ResourceTypeCluster,
			ResourceName: "*",
			Operation:    OperationDescribe,
			Permission:   PermissionAllow,
			CreatedAt:    now,
		},
		{
			ID:           "acl-deny-billing-consumer-audit",
			Principal:    "User:billing_consumer",
			ResourceType: ResourceTypeTopic,
			ResourceName: "audit-*",
			Operation:    OperationWrite,
			Permission:   PermissionDeny,
			CreatedAt:    now,
		},
	}
	for _, r := range defaultRules {
		m.rules[r.ID] = r
	}
}

func (m *AclManager) AddRule(rule *AclRule) error {
	if rule == nil {
		return errors.New("rule cannot be nil")
	}
	if strings.TrimSpace(rule.Principal) == "" {
		return errors.New("principal is required")
	}
	if rule.ResourceType == "" {
		return errors.New("resource_type is required")
	}
	if strings.TrimSpace(rule.ResourceName) == "" {
		return errors.New("resource_name is required")
	}
	if rule.Operation == "" {
		return errors.New("operation is required")
	}
	if rule.Permission != PermissionAllow && rule.Permission != PermissionDeny {
		return errors.New("permission must be ALLOW or DENY")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if rule.ID == "" {
		rule.ID = generateID("acl")
	}
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = time.Now().UTC()
	}

	// Normalise principal prefix if missing
	if !strings.HasPrefix(rule.Principal, "User:") && rule.Principal != "*" {
		rule.Principal = "User:" + strings.TrimSpace(rule.Principal)
	}

	// Store a copy
	copied := *rule
	m.rules[rule.ID] = &copied
	return nil
}

func (m *AclManager) ListRules() []*AclRule {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rules := make([]*AclRule, 0, len(m.rules))
	for _, r := range m.rules {
		copied := *r
		rules = append(rules, &copied)
	}

	// Deterministic sort: CreatedAt ascending, then ID
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].CreatedAt.Equal(rules[j].CreatedAt) {
			return rules[i].ID < rules[j].ID
		}
		return rules[i].CreatedAt.Before(rules[j].CreatedAt)
	})

	return rules
}

func (m *AclManager) DeleteRule(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.rules[id]; !exists {
		return errors.New("rule not found")
	}
	delete(m.rules, id)
	return nil
}

func (m *AclManager) AddUser(u *User) error {
	if u == nil {
		return errors.New("user cannot be nil")
	}
	username := strings.TrimSpace(u.Username)
	if username == "" {
		return errors.New("username is required")
	}
	// Strip "User:" prefix if present for username storage
	username = strings.TrimPrefix(username, "User:")

	switch u.Role {
	case RoleSuperAdmin, RoleOperator, RoleProducer, RoleConsumer, RoleAuditor:
		// valid
	default:
		return fmt.Errorf("invalid role: %s", u.Role)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if u.CreatedAt.IsZero() {
		u.CreatedAt = time.Now().UTC()
	}

	copied := *u
	copied.Username = username
	m.users[username] = &copied
	return nil
}

func (m *AclManager) ListUsers() []*User {
	m.mu.RLock()
	defer m.mu.RUnlock()

	users := make([]*User, 0, len(m.users))
	for _, u := range m.users {
		copied := *u
		users = append(users, &copied)
	}

	// Deterministic sort: Username ascending
	sort.Slice(users, func(i, j int) bool {
		return users[i].Username < users[j].Username
	})

	return users
}

func (m *AclManager) GetUser(username string) (*User, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cleanName := strings.TrimPrefix(strings.TrimSpace(username), "User:")
	u, ok := m.users[cleanName]
	if !ok {
		return nil, false
	}
	copied := *u
	return &copied, true
}

func (m *AclManager) Authorize(principal string, rType ResourceType, rName string, op Operation) bool {
	allowed, _ := m.AuthorizeWithReason(principal, rType, rName, op)
	return allowed
}

func (m *AclManager) AuthorizeWithReason(principal string, rType ResourceType, rName string, op Operation) (bool, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cleanPrincipal := strings.TrimSpace(principal)
	cleanUsername := strings.TrimPrefix(cleanPrincipal, "User:")

	// 1. Gather all matching rules
	var matchingRules []*AclRule
	for _, r := range m.rules {
		if ruleMatches(r, cleanPrincipal, rType, rName, op) {
			matchingRules = append(matchingRules, r)
		}
	}

	// 2. High-Precedence Rule: Explicit DENY always takes precedence over ALLOW and SUPER_ADMIN
	for _, r := range matchingRules {
		if r.Permission == PermissionDeny {
			return false, fmt.Sprintf("Explicit DENY rule matched (rule: %s, principal: %s, resource: %s:%s, op: %s)",
				r.ID, r.Principal, r.ResourceType, r.ResourceName, r.Operation)
		}
	}

	// 3. Role Check: SUPER_ADMIN has global privileges across cluster, topics, and groups
	if u, exists := m.users[cleanUsername]; exists && u.Role == RoleSuperAdmin {
		return true, fmt.Sprintf("Access granted: principal '%s' holds SUPER_ADMIN role with unrestricted cluster privileges", cleanUsername)
	}

	// 4. Explicit ALLOW match
	for _, r := range matchingRules {
		if r.Permission == PermissionAllow {
			return true, fmt.Sprintf("Access granted by rule %s (principal: %s, resource: %s:%s, op: %s)",
				r.ID, r.Principal, r.ResourceType, r.ResourceName, r.Operation)
		}
	}

	// 5. Zero-Trust Default Deny
	return false, fmt.Sprintf("Zero-trust denial: no matching ALLOW policy found for principal '%s' on %s '%s' for operation %s",
		cleanPrincipal, rType, rName, op)
}

func ruleMatches(rule *AclRule, queryPrincipal string, qType ResourceType, qName string, qOp Operation) bool {
	// 1. Principal match
	if !principalMatches(rule.Principal, queryPrincipal) {
		return false
	}

	// 2. Resource Type match
	if rule.ResourceType != qType && rule.ResourceType != "ALL" && rule.ResourceType != "" {
		return false
	}

	// 3. Resource Name match (supports *, orders-*, exact)
	if !resourceNameMatches(rule.ResourceName, qName) {
		return false
	}

	// 4. Operation match (OperationAll grants any operation)
	if rule.Operation != OperationAll && rule.Operation != qOp {
		return false
	}

	return true
}

func principalMatches(rulePrincipal, queryPrincipal string) bool {
	rClean := strings.TrimPrefix(strings.TrimSpace(rulePrincipal), "User:")
	qClean := strings.TrimPrefix(strings.TrimSpace(queryPrincipal), "User:")

	if rClean == "*" || rulePrincipal == "*" || rulePrincipal == "User:*" {
		return true
	}

	if strings.EqualFold(rClean, qClean) {
		return true
	}

	// Wildcard pattern on principal e.g. "service-*"
	if matched, err := filepath.Match(rClean, qClean); err == nil && matched {
		return true
	}

	return false
}

func resourceNameMatches(rulePattern, targetName string) bool {
	if rulePattern == "*" {
		return true
	}

	if rulePattern == targetName {
		return true
	}

	// Prefix match like "orders-*"
	if strings.HasSuffix(rulePattern, "*") {
		prefix := strings.TrimSuffix(rulePattern, "*")
		if strings.HasPrefix(targetName, prefix) {
			return true
		}
	}

	// General glob match
	if matched, err := filepath.Match(rulePattern, targetName); err == nil && matched {
		return true
	}

	return false
}

func generateID(prefix string) string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}
