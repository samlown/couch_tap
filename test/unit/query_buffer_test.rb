
require 'test_helper'

class QueryBufferTest < Test::Unit::TestCase

  def test_insert_adds_data_to_be_inserted
    buffer = CouchTap::QueryBuffer.new

    id = 123
    data = { a: 1, b: 'b' }

    entity = mock(top_level: true)
    entity.expects(:insert).with(id, data)

    CouchTap::Entity.expects(:new).with(:item, true).returns(entity)

    buffer.insert(item_to_insert(true, 123, data))
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

    CouchTap::Entity.expects(:new).with(:item, true).returns(entity)

    buffer.insert(item_to_insert(true, id, data))
    buffer.insert(item_to_insert(true, id2, data2))
  end

  def test_insert_cannot_have_same_entity_as_both_top_level_and_dependent
    buffer = CouchTap::QueryBuffer.new

    buffer.insert(item_to_insert(true, 123))
    assert_raises ArgumentError do
      buffer.insert(item_to_insert(false, 123))
    end
  end

  def test_insert_increases_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(item_to_insert(true, 123))
    assert_equal 2, buffer.insert(item_to_insert(true, 123))
  end

  def test_delete_adds_data_to_be_deleted
    buffer = CouchTap::QueryBuffer.new

    id = 123
    key = :item_id

    entity = mock(top_level: true)
    entity.expects(:delete).with(key, id)

    CouchTap::Entity.expects(:new).with(:item, true).returns(entity)

    buffer.delete(item_to_delete(id))
  end

  def test_delete_reuses_entities_with_same_name
    buffer = CouchTap::QueryBuffer.new

    id = 123
    id2 = 987
    key = :item_id

    entity = mock()
    entity.expects(:top_level).twice.returns(true)
    entity.expects(:delete).with(key, id)
    entity.expects(:delete).with(key, id2)

    CouchTap::Entity.expects(:new).with(:item, true).returns(entity)

    buffer.delete(item_to_delete(id))
    buffer.delete(item_to_delete(id2))
  end

  def test_delete_cannot_have_same_entity_as_both_top_level_and_dependent
    buffer = CouchTap::QueryBuffer.new

    buffer.delete(item_to_delete(123))
    assert_raises ArgumentError do
      buffer.delete(item_to_delete(987, false))
    end
  end

  def test_delete_increases_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.delete(item_to_delete(123))
    assert_equal 2, buffer.delete(item_to_delete(987))
  end

  def test_clear_resets_the_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(item_to_insert(true, 123))
    assert_equal 2, buffer.delete(item_to_delete(987))
    buffer.clear
    assert_equal 1, buffer.insert(item_to_insert(true, 123))
  end

  def test_clear_clears_the_buffer
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(item_to_insert(true, 123))
    assert_equal 2, buffer.delete(CouchTap::Operations::DeleteOperation.new(:another, true, :another_id, 987))

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

    assert_equal 1, buffer.insert(item_to_insert(true, 123))
    assert_equal 2, buffer.delete(CouchTap::Operations::DeleteOperation.new(:another, true, :another_id, 987))

    entity_names = []
    buffer.each { |e| entity_names << e.name }
    assert_equal %i(item another), entity_names
  end

  private

  def item_to_insert(top_level, id, data = nil)
    data ||= { item_id: id, name: 'dummy', count: rand() }
    CouchTap::Operations::InsertOperation.new(:item, top_level, id, data)
  end

  def item_to_delete(id)
    CouchTap::Operations::DeleteOperation.new(:item, true, :item_id, id)
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