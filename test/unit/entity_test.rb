
require 'test_helper'

class EntityTest < Test::Unit::TestCase

  def test_insert_saves_a_top_level_entity
    id = 123
    data = { id: id, a: 1, b: "b" }
    entity = CouchTap::Entity.new('dummy', true)
    entity.insert(id, data)
    assert_equal [[id, 1, "b"]], entity.insert_values(data.keys)
  end

  def test_insert_saves_a_non_top_level
    id = 123
    data = { id: id, a: 1, b: "b" }
    entity = CouchTap::Entity.new('dummy', false)
    entity.insert(id, data)
    assert_equal [[id, 1, "b"]], entity.insert_values(data.keys)
  end

  def test_insert_overrides_a_top_level_with_same_key
    id = 123
    data = { id: id, a: 1, b: "b" }
    data2 = { id: id, a: 2, b: "c" }
    entity = CouchTap::Entity.new('dummy', true)

    entity.insert(id, data)
    entity.insert(id, data2)

    assert_equal [[id, 2, "c"]], entity.insert_values(data.keys)
  end

  def test_insert_appends_a_non_top_level
    id = 123
    data = { id: id, a: 1, b: "b" }
    data2 = { id: id, a: 2, b: "c" }
    entity = CouchTap::Entity.new('dummy', false)

    entity.insert(id, data)
    entity.insert(id, data2)

    assert_equal [[id, 1, "b"], [id, 2, "c"]], entity.insert_values(data.keys)
  end

  def test_insert_values_brings_nil_for_unmatched_keys
    id = 123
    data = { id: id, a: 1, b: "b" }
    entity = CouchTap::Entity.new('dummy', true)

    entity.insert(id, data)

    assert_equal [[nil, id, nil, 1, nil, "b", nil]], entity.insert_values([:c, :id, :d, :a, :e, :b, :f])
  end

  def test_delete_saves_the_entity_id
    id = 123
    id2 = 234
    entity = CouchTap::Entity.new('dummy', true)

    entity.delete('dummy_id', id)
    entity.delete('dummy_id', id2)

    assert_equal [id, id2], entity.deletes
  end

  def test_delete_removes_duplicate_ids
    id = 123
    entity = CouchTap::Entity.new('dummy', true)

    entity.delete('dummy_id', id)
    entity.delete('dummy_id', id)

    assert_equal [id], entity.deletes
  end

  def test_delete_sets_the_pk
    key = 'dummy_id'
    entity = CouchTap::Entity.new('dummy', true)
    entity.delete(key, 123)

    assert_equal key, entity.primary_key
  end

  def test_delete_protects_against_different_pks
    key = 'dummy_id'
    entity = CouchTap::Entity.new('dummy', true)
    entity.delete(key, 123)

    assert_equal key, entity.primary_key
    assert_raises RuntimeError  do
      entity.delete('another_key', 123)
    end
  end
end

