from django.urls import path
from .views import ReconciliationView, ResultsDetailView

urlpatterns = [
    path('reconcile/', ReconciliationView.as_view(), name='reconcile'),
    path('results/<int:report_id>/', ResultsDetailView.as_view(), name='results-detail'),
]