
require 'test_helper'

class QueryBufferTest < Test::Unit::TestCase

  def test_insert_adds_data_to_be_inserted
    buffer = CouchTap::QueryBuffer.new

    id = 123
    data = { a: 1, b: 'b' }

    entity = mock(top_level: true)
    entity.expects(:insert).with(id, data)

    CouchTap::Entity.expects(:new).with('dummy', true).returns(entity)

    buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, id, data))
  end

  def test_insert_reuses_entities_with_same_name
    buffer = CouchTap::QueryBuffer.new

    id = 123
    data = { a: 1, b: 'b' }

    id2 = 987
    data2 = { a: 2, b: 'c' }

    entity = mock()
    entity.expects(:top_level).twice.returns(true)
    entity.expects(:insert).with(id, data)
    entity.expects(:insert).with(id2, data2)

    CouchTap::Entity.expects(:new).with('dummy', true).returns(entity)

    buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, id, data))
    buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, id2, data2))
  end

  def test_insert_cannot_have_same_entity_as_both_top_level_and_dependent
    buffer = CouchTap::QueryBuffer.new

    buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, 123, a: 1, b: 'b'))
    assert_raises ArgumentError do
      buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', false, 123, a: 1, b: 'b'))
    end
  end

  def test_insert_increases_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, 123, a: 1, b: 'b'))
    assert_equal 2, buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, 123, a: 1, b: 'b'))
  end

  def test_delete_adds_data_to_be_deleted
    buffer = CouchTap::QueryBuffer.new

    id = 123
    key = 'dummy_id'

    entity = mock(top_level: true)
    entity.expects(:delete).with(key, id)

    CouchTap::Entity.expects(:new).with('dummy', true).returns(entity)

    buffer.delete('dummy', true, key, id)
  end

  def test_delete_reuses_entities_with_same_name
    buffer = CouchTap::QueryBuffer.new

    id = 123
    id2 = 987
    key = 'dummy_id'

    entity = mock()
    entity.expects(:top_level).twice.returns(true)
    entity.expects(:delete).with(key, id)
    entity.expects(:delete).with(key, id2)

    CouchTap::Entity.expects(:new).with('dummy', true).returns(entity)

    buffer.delete('dummy', true, key, id)
    buffer.delete('dummy', true, key, id2)
  end

  def test_delete_cannot_have_same_entity_as_both_top_level_and_dependent
    buffer = CouchTap::QueryBuffer.new

    buffer.delete('dummy', true, 'dummy_id', 123)
    assert_raises ArgumentError do
      buffer.delete('dummy', false, 'dummy_id',  987)
    end
  end

  def test_delete_increases_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.delete('dummy', true, 'dummy_id', 123)
    assert_equal 2, buffer.delete('dummy', true, 'dummy_id', 987)
  end

  def test_clear_resets_the_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, 123, a: 1, b: 'c'))
    assert_equal 2, buffer.delete('dummy', true, 'dummy_id', 987)
    buffer.clear
    assert_equal 1, buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, 123, a: 1, b: 'c'))
  end

  def test_clear_clears_the_buffer
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, 123, a: 1, b: 'c'))
    assert_equal 2, buffer.delete('another', true, 'another_id', 987)

    items = 0
    buffer.each { |i| items += 1 }
    assert_equal 2, items

    buffer.clear

    items = 0
    buffer.each { |i| items += 1 }
    assert_equal 0, items
  end

  def test_can_iterate_over_entities
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(CouchTap::Operations::InsertOperation.new('dummy', true, 123, a: 1, b: 'c'))
    assert_equal 2, buffer.delete('another', true, 'another_id', 987)

    entity_names = []
    buffer.each { |e| entity_names << e.name }
    assert_equal %w(dummy another), entity_names
  end

  def test_top_level_items_are_overwritten
    buffer = CouchTap::QueryBuffer.new

    buffer.insert('dummy', true, 123, a: 1, b: 'c')
    buffer.insert('dummy', true, 123, a: 2, b: 'd')

    entities = []
    buffer.each { |e| entities << e }

    assert_equal 1, entities.count
    assert_equal [[2, 'd']], entities.first.insert_values(%i(a b))
  end

  def test_child_elements_are_deleted
    buffer = CouchTap::QueryBuffer.new

    buffer.insert('dummy', false, 123, a: 1, b: 'c')
    buffer.delete('dummy', false, 'dummy_id', 123)

    entities = []
    buffer.each { |e| entities << e }

    assert_equal 1, entities.count
    refute entities.first.any_insert?
  end
end
