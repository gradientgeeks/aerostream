package auth_test

import (
	"testing"
	"time"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/auth"
)

func TestNewAclManager_DefaultSeeds(t *testing.T) {
	mgr := auth.NewAclManager()

	users := mgr.ListUsers()
	if len(users) < 4 {
		t.Fatalf("expected at least 4 default users, got %d", len(users))
	}

	rules := mgr.ListRules()
	if len(rules) < 6 {
		t.Fatalf("expected at least 6 default rules, got %d", len(rules))
	}

	// Verify admin is SUPER_ADMIN
	admin, ok := mgr.GetUser("admin")
	if !ok || admin.Role != auth.RoleSuperAdmin {
		t.Fatalf("expected admin to have SUPER_ADMIN role, got %+v", admin)
	}

	// Verify order_producer is PRODUCER
	producer, ok := mgr.GetUser("order_producer")
	if !ok || producer.Role != auth.RoleProducer {
		t.Fatalf("expected order_producer to have PRODUCER role, got %+v", producer)
	}
}

func TestAclManager_WildcardPatterns(t *testing.T) {
	mgr := auth.NewAclManager()

	// Default seed: order_producer can WRITE to "orders-*"
	// Positive matches:
	if !mgr.Authorize("User:order_producer", auth.ResourceTypeTopic, "orders-europe", auth.OperationWrite) {
		t.Errorf("expected order_producer to be authorized to write to orders-europe")
	}
	if !mgr.Authorize("order_producer", auth.ResourceTypeTopic, "orders-us-west", auth.OperationWrite) {
		t.Errorf("expected order_producer (without User: prefix) to write to orders-us-west")
	}
	if !mgr.Authorize("User:order_producer", auth.ResourceTypeTopic, "orders-", auth.OperationWrite) {
		t.Errorf("expected order_producer to write to orders-")
	}

	// Negative matches (not matching prefix):
	if mgr.Authorize("User:order_producer", auth.ResourceTypeTopic, "payments-eu", auth.OperationWrite) {
		t.Errorf("expected order_producer to be denied writing to payments-eu")
	}
	if mgr.Authorize("User:order_producer", auth.ResourceTypeTopic, "pre-orders-123", auth.OperationWrite) {
		t.Errorf("expected order_producer to be denied writing to pre-orders-123")
	}
}

func TestAclManager_AllowVsDenyPrecedence(t *testing.T) {
	mgr := auth.NewAclManager()

	// Add an explicit ALLOW for Alice on secret-*
	err := mgr.AddRule(&auth.AclRule{
		ID:           "rule-allow-alice",
		Principal:    "User:alice",
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "secret-*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to add allow rule: %v", err)
	}

	// Before DENY: Alice can read secret-keys
	if !mgr.Authorize("User:alice", auth.ResourceTypeTopic, "secret-keys", auth.OperationRead) {
		t.Fatalf("expected Alice to be allowed reading secret-keys")
	}

	// Now add an explicit DENY for Alice on secret-keys
	err = mgr.AddRule(&auth.AclRule{
		ID:           "rule-deny-alice",
		Principal:    "User:alice",
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "secret-keys",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionDeny,
	})
	if err != nil {
		t.Fatalf("failed to add deny rule: %v", err)
	}

	// Explicit DENY must override ALLOW on secret-keys!
	allowed, reason := mgr.AuthorizeWithReason("User:alice", auth.ResourceTypeTopic, "secret-keys", auth.OperationRead)
	if allowed {
		t.Errorf("expected Alice to be DENIED on secret-keys due to explicit DENY precedence, reason: %s", reason)
	}

	// But Alice can still read other secret-* topics like secret-tokens
	if !mgr.Authorize("User:alice", auth.ResourceTypeTopic, "secret-tokens", auth.OperationRead) {
		t.Errorf("expected Alice to still be allowed reading secret-tokens")
	}
}

func TestAclManager_RoleChecks(t *testing.T) {
	mgr := auth.NewAclManager()

	// 1. SUPER_ADMIN (admin) should be authorized across any topic and operation
	allowed, reason := mgr.AuthorizeWithReason("User:admin", auth.ResourceTypeTopic, "any-unlisted-topic", auth.OperationAlter)
	if !allowed {
		t.Errorf("expected admin (SUPER_ADMIN) to be authorized, got reason: %s", reason)
	}
	allowed, _ = mgr.AuthorizeWithReason("admin", auth.ResourceTypeCluster, "cluster-config", auth.OperationWrite)
	if !allowed {
		t.Errorf("expected admin without User: prefix to be authorized for cluster write")
	}

	// 2. AUDITOR (auditor) has DESCRIBE on * across topics, groups, and cluster
	if !mgr.Authorize("User:auditor", auth.ResourceTypeTopic, "production-logs", auth.OperationDescribe) {
		t.Errorf("expected auditor to be allowed to DESCRIBE topics")
	}
	if !mgr.Authorize("User:auditor", auth.ResourceTypeGroup, "orders-group", auth.OperationDescribe) {
		t.Errorf("expected auditor to be allowed to DESCRIBE groups")
	}
	// Auditor must NOT be allowed to WRITE or ALTER
	if mgr.Authorize("User:auditor", auth.ResourceTypeTopic, "production-logs", auth.OperationWrite) {
		t.Errorf("auditor should NOT be allowed to WRITE topics")
	}
	if mgr.Authorize("User:auditor", auth.ResourceTypeCluster, "cluster", auth.OperationAlter) {
		t.Errorf("auditor should NOT be allowed to ALTER cluster")
	}

	// 3. CONSUMER (billing_consumer) can read orders-* and group billing-group
	if !mgr.Authorize("User:billing_consumer", auth.ResourceTypeTopic, "orders-2026", auth.OperationRead) {
		t.Errorf("expected billing_consumer to read orders-2026")
	}
	if !mgr.Authorize("User:billing_consumer", auth.ResourceTypeGroup, "billing-group", auth.OperationRead) {
		t.Errorf("expected billing_consumer to read billing-group")
	}
	// billing_consumer cannot write to orders-*
	if mgr.Authorize("User:billing_consumer", auth.ResourceTypeTopic, "orders-2026", auth.OperationWrite) {
		t.Errorf("billing_consumer should NOT be allowed to write orders-2026")
	}
	// billing_consumer has an explicit DENY on audit-*
	if mgr.Authorize("User:billing_consumer", auth.ResourceTypeTopic, "audit-events", auth.OperationWrite) {
		t.Errorf("billing_consumer should be DENIED writing audit-events")
	}

	// 4. Unknown user should be denied by default zero-trust policy
	unknownAllowed, unknownReason := mgr.AuthorizeWithReason("User:stranger", auth.ResourceTypeTopic, "orders-2026", auth.OperationRead)
	if unknownAllowed {
		t.Errorf("expected unknown user to be denied by default zero-trust")
	}
	if unknownReason == "" {
		t.Errorf("expected non-empty reason for zero-trust denial")
	}
}

func TestAclManager_RuleCRUD(t *testing.T) {
	mgr := auth.NewAclManager()

	initialCount := len(mgr.ListRules())

	rule := &auth.AclRule{
		Principal:    "User:charlie",
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "telemetry",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	}

	if err := mgr.AddRule(rule); err != nil {
		t.Fatalf("failed to add rule: %v", err)
	}

	if rule.ID == "" {
		t.Errorf("expected generated ID on added rule")
	}

	if len(mgr.ListRules()) != initialCount+1 {
		t.Errorf("expected %d rules, got %d", initialCount+1, len(mgr.ListRules()))
	}

	// Delete rule
	if err := mgr.DeleteRule(rule.ID); err != nil {
		t.Fatalf("failed to delete rule: %v", err)
	}

	if len(mgr.ListRules()) != initialCount {
		t.Errorf("expected %d rules after delete, got %d", initialCount, len(mgr.ListRules()))
	}

	// Deleting again should return error
	if err := mgr.DeleteRule(rule.ID); err == nil {
		t.Errorf("expected error when deleting non-existent rule")
	}
}

func TestAclManager_UserCRUD(t *testing.T) {
	mgr := auth.NewAclManager()

	newUser := &auth.User{
		Username:  "developer_dave",
		Role:      auth.RoleProducer,
		CreatedAt: time.Now(),
	}

	if err := mgr.AddUser(newUser); err != nil {
		t.Fatalf("failed to add user: %v", err)
	}

	fetched, ok := mgr.GetUser("developer_dave")
	if !ok || fetched.Role != auth.RoleProducer {
		t.Errorf("expected to fetch developer_dave with RoleProducer")
	}

	// Invalid role check
	invalidUser := &auth.User{
		Username: "bad_user",
		Role:     "INVALID_ROLE",
	}
	if err := mgr.AddUser(invalidUser); err == nil {
		t.Errorf("expected error adding user with invalid role")
	}
}
