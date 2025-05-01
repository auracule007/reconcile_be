from django.db import models

class ReconciliationReport(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    system_a_count = models.IntegerField()
    system_b_count = models.IntegerField()
    processing_time = models.FloatField()
    
    class Meta:
        ordering = ['-created_at']

class Discrepancy(models.Model):
    DISCREPANCY_TYPES = [
        ('missing_in_b', 'Missing in System B'),
        ('missing_in_a', 'Missing in System A'),
        ('amount_mismatch', 'Amount Mismatch'),
        ('status_mismatch', 'Status Mismatch'),
    ]
    
    report = models.ForeignKey(ReconciliationReport, on_delete=models.CASCADE, related_name='discrepancies')
    transaction_id = models.CharField(max_length=255)
    discrepancy_type = models.CharField(max_length=20, choices=DISCREPANCY_TYPES)
    amount_a = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    amount_b = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    status_a = models.CharField(max_length=50, null=True, blank=True)
    status_b = models.CharField(max_length=50, null=True, blank=True)
    currency = models.CharField(max_length=3, null=True, blank=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['transaction_id']),
            models.Index(fields=['discrepancy_type']),
        ]