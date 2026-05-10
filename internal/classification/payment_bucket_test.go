package classification

import (
	"context"
	"testing"
)

func TestAssignBucket_SenderEmail(t *testing.T) {
	router := NewRuleBasedPaymentBucketRouter(DefaultPaymentBucketRules())

	bucket, matchedRule, err := router.AssignBucket(context.Background(), makeMsg("MESSENGER@MESSAGING.SQUAREUP.COM", "messaging.squareup.com", "Invoice enclosed"), CategoryVendor, IntentInvoice)
	if err != nil {
		t.Fatalf("AssignBucket: %v", err)
	}
	if bucket != BucketPayableInvoiceAutomated {
		t.Fatalf("bucket: got %q, want %q", bucket, BucketPayableInvoiceAutomated)
	}
	if matchedRule != "bucket_rule:sender_email:messenger@messaging.squareup.com" {
		t.Fatalf("matchedRule: got %q", matchedRule)
	}
}

func TestAssignBucket_Domain(t *testing.T) {
	router := NewRuleBasedPaymentBucketRouter(DefaultPaymentBucketRules())

	bucket, matchedRule, err := router.AssignBucket(context.Background(), makeMsg("agent@AVANTEINS.COM", "AVANTEINS.COM", "Coverage update"), CategoryVendor, "")
	if err != nil {
		t.Fatalf("AssignBucket: %v", err)
	}
	if bucket != BucketPayableUrgentInsurance {
		t.Fatalf("bucket: got %q, want %q", bucket, BucketPayableUrgentInsurance)
	}
	if matchedRule != "bucket_rule:domain:avanteins.com" {
		t.Fatalf("matchedRule: got %q", matchedRule)
	}
}

func TestAssignBucket_SubjectKeywords_Overdue(t *testing.T) {
	router := NewRuleBasedPaymentBucketRouter(DefaultPaymentBucketRules())

	bucket, matchedRule, err := router.AssignBucket(context.Background(), makeMsg("unknown@example.com", "example.com", "Re: Overdue Balance Notice"), CategoryUnknown, "")
	if err != nil {
		t.Fatalf("AssignBucket: %v", err)
	}
	if bucket != BucketPayableOverdueVendor {
		t.Fatalf("bucket: got %q, want %q", bucket, BucketPayableOverdueVendor)
	}
	if matchedRule != "bucket_rule:subject_keywords:overdue,obligation" {
		t.Fatalf("matchedRule: got %q", matchedRule)
	}
}

func TestAssignBucket_SubjectKeywords_Statement(t *testing.T) {
	router := NewRuleBasedPaymentBucketRouter(DefaultPaymentBucketRules())

	bucket, matchedRule, err := router.AssignBucket(context.Background(), makeMsg("unknown@example.com", "example.com", "Statement of Account 2026"), CategoryUnknown, "")
	if err != nil {
		t.Fatalf("AssignBucket: %v", err)
	}
	if bucket != BucketStatementConfirmation {
		t.Fatalf("bucket: got %q, want %q", bucket, BucketStatementConfirmation)
	}
	if matchedRule != "bucket_rule:subject_keywords:statement" {
		t.Fatalf("matchedRule: got %q", matchedRule)
	}
}

func TestAssignBucket_CategoryIntent(t *testing.T) {
	router := NewRuleBasedPaymentBucketRouter(DefaultPaymentBucketRules())

	bucket, matchedRule, err := router.AssignBucket(context.Background(), makeMsg("vendor@example.com", "example.com", "Invoice #442"), CategoryVendor, IntentInvoice)
	if err != nil {
		t.Fatalf("AssignBucket: %v", err)
	}
	if bucket != BucketPayableInvoiceVendor {
		t.Fatalf("bucket: got %q, want %q", bucket, BucketPayableInvoiceVendor)
	}
	if matchedRule != "bucket_rule:category_intent:vendor:invoice" {
		t.Fatalf("matchedRule: got %q", matchedRule)
	}
}

func TestAssignBucket_None(t *testing.T) {
	router := NewRuleBasedPaymentBucketRouter(DefaultPaymentBucketRules())

	bucket, matchedRule, err := router.AssignBucket(context.Background(), makeMsg("person@example.com", "example.com", "Meeting notes"), CategoryClient, "")
	if err != nil {
		t.Fatalf("AssignBucket: %v", err)
	}
	if bucket != BucketNone {
		t.Fatalf("bucket: got %q, want empty", bucket)
	}
	if matchedRule != "" {
		t.Fatalf("matchedRule: got %q, want empty", matchedRule)
	}
}

func TestAssignBucket_PriorityOrder(t *testing.T) {
	router := NewRuleBasedPaymentBucketRouter([]PaymentBucketRule{
		{Bucket: BucketPayableUrgentInsurance, Priority: 2, PatternType: BucketPatternDomain, PatternValue: "avanteins.com"},
		{Bucket: BucketPayableInvoiceAutomated, Priority: 1, PatternType: BucketPatternSenderEmail, PatternValue: "agent@avanteins.com"},
	})

	bucket, matchedRule, err := router.AssignBucket(context.Background(), makeMsg("agent@avanteins.com", "avanteins.com", "Payment required"), CategoryVendor, "")
	if err != nil {
		t.Fatalf("AssignBucket: %v", err)
	}
	if bucket != BucketPayableInvoiceAutomated {
		t.Fatalf("bucket: got %q, want %q", bucket, BucketPayableInvoiceAutomated)
	}
	if matchedRule != "bucket_rule:sender_email:agent@avanteins.com" {
		t.Fatalf("matchedRule: got %q", matchedRule)
	}
}

func TestDefaultPaymentBucketRules_Count(t *testing.T) {
	rules := DefaultPaymentBucketRules()
	if len(rules) != 10 {
		t.Fatalf("len(DefaultPaymentBucketRules()): got %d, want 10", len(rules))
	}
}
