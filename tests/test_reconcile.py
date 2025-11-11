"""Tests for reconciliation logic."""

import pytest

from corpus.models import RawEntity, Provenance
from corpus.reconcile import EntityReconciler


def test_entity_reconciler_add_entities():
    """Test adding entities with priorities."""
    reconciler = EntityReconciler()
    
    # Add base entity
    base_entity = RawEntity(
        entity_type="weapon",
        name="Test Sword",
        description="Base description",
        provenance=[Provenance(source="base", uri="test://base")],
    )
    
    reconciler.add_entities([base_entity], priority=2)
    assert len(reconciler.entity_map) == 1
    
    # Add higher priority entity with same key
    dlc_entity = RawEntity(
        entity_type="weapon",
        name="Test Sword",
        description="DLC description",
        is_dlc=True,
        provenance=[Provenance(source="dlc", uri="test://dlc")],
    )
    
    reconciler.add_entities([dlc_entity], priority=1)
    
    # Should still be 1 entity (merged)
    assert len(reconciler.entity_map) == 1
    
    # Should use higher priority (DLC) description
    entities = reconciler.get_reconciled_entities()
    assert entities[0].description == "DLC description"
    assert entities[0].is_dlc is True
    
    # Should have both provenances
    sources = [p.source for p in entities[0].provenance]
    assert "dlc" in sources


def test_entity_reconciler_different_types():
    """Test that different entity types don't merge."""
    reconciler = EntityReconciler()
    
    weapon = RawEntity(
        entity_type="weapon",
        name="Test Item",
        description="A weapon",
        provenance=[Provenance(source="test", uri="test://weapon")],
    )
    
    armor = RawEntity(
        entity_type="armor",
        name="Test Item",
        description="An armor piece",
        provenance=[Provenance(source="test", uri="test://armor")],
    )
    
    reconciler.add_entities([weapon, armor], priority=1)
    
    # Should be 2 separate entities
    assert len(reconciler.entity_map) == 2
    
    entities = reconciler.get_reconciled_entities()
    types = [e.entity_type for e in entities]
    assert "weapon" in types
    assert "armor" in types


def test_text_matching_threshold():
    """Test fuzzy text matching with threshold."""
    reconciler = EntityReconciler(threshold=0.8)
    
    # Add a base entity
    entity = RawEntity(
        entity_type="boss",
        name="Starscourge Radahn",
        description="A fearsome general",
        provenance=[Provenance(source="base", uri="test://base")],
    )
    
    reconciler.add_entities([entity], priority=1)
    
    # Add text snippets with varying similarity
    exact_match = RawEntity(
        entity_type="text_snippet",
        name="Starscourge Radahn",
        description="Additional lore text",
        provenance=[Provenance(source="dlc_text", uri="test://dlc")],
    )
    
    close_match = RawEntity(
        entity_type="text_snippet",
        name="Radahn",  # Partial match
        description="More lore",
        provenance=[Provenance(source="dlc_text", uri="test://dlc")],
    )
    
    no_match = RawEntity(
        entity_type="text_snippet",
        name="Completely Different Boss",
        description="Unrelated",
        provenance=[Provenance(source="dlc_text", uri="test://dlc")],
    )
    
    matched, unmapped = reconciler.match_text_to_entities(
        [exact_match, close_match, no_match]
    )
    
    # Exact match should succeed
    assert "Starscourge Radahn" in matched
    
    # No match should be in unmapped
    assert any(e.name == "Completely Different Boss" for e in unmapped)
