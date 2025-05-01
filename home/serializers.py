from rest_framework import serializers
from .models import ReconciliationReport, Discrepancy

class DiscrepancySerializer(serializers.ModelSerializer):
    class Meta:
        model = Discrepancy
        fields = '__all__'

class ReconciliationReportSerializer(serializers.ModelSerializer):
    discrepancies = DiscrepancySerializer(many=True, read_only=True)
    
    class Meta:
        model = ReconciliationReport
        fields = '__all__'