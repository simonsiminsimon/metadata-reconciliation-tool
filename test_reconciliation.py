# test_reconciliation.py
from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
from app.services.reconciliation_engine import Entity, EntityType

# Initialize engine with verbose settings
engine = EnhancedReconciliationEngine()

# Test with a simple entity
test_entity = Entity(
    id="test_1",
    name="William Shakespeare",
    entity_type=EntityType.PERSON,
    context={},
    source_row=0
)

print("Testing reconciliation...")
results = engine.process_entities([test_entity])

print(f"Results: {len(results)}")
for result in results:
    print(f"Entity: {result.entity.name}")
    print(f"Matches: {len(result.matches)}")
    print(f"Best match: {result.best_match}")
    print(f"Confidence: {result.confidence}")