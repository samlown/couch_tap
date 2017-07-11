require 'test_helper'

class QueryBufferTest < Test::Unit::TestCase

  def test_insert_adds_data_to_be_inserted
    buffer = CouchTap::QueryBuffer.new

    id = 123
    data = { a: 1, b: 'b' }

    entity = mock(top_level: true)
    entity.expects(:insert).with(:item_id, id, data)

    CouchTap::Entity.expects(:new).with(:item, true).returns(entity)

    buffer.insert(item_to_insert(true, :item_id, 123, data))
  end

  def test_insert_reuses_entities_with_same_name
    buffer = CouchTap::QueryBuffer.new

    id = 123
    data = { a: 1, b: 'b' }

    id2 = 987
    data2 = { a: 2, b: 'c' }

    entity = mock()
    entity.expects(:top_level).twice.returns(true)
    entity.expects(:insert).with(:item_id, id, data)
    entity.expects(:insert).with(:item_id, id2, data2)

    CouchTap::Entity.expects(:new).with(:item, true).returns(entity)

    buffer.insert(item_to_insert(true, :item_id, id, data))
    buffer.insert(item_to_insert(true, :item_id, id2, data2))
  end

  def test_insert_cannot_have_same_entity_as_both_top_level_and_dependent
    buffer = CouchTap::QueryBuffer.new

    buffer.insert(item_to_insert(true, :item_id, 123))
    assert_raises ArgumentError do
      buffer.insert(item_to_insert(false, :item_id, 123))
    end
  end

  def test_insert_increases_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(item_to_insert(true, :item_id, 123))
    assert_equal 2, buffer.insert(item_to_insert(true, :item_id, 123))
  end

  def test_delete_adds_data_to_be_deleted
    buffer = CouchTap::QueryBuffer.new

    id = 123
    key = :item_id

    entity = mock(top_level: true)
    entity.expects(:delete).with(key, id)

    CouchTap::Entity.expects(:new).with(:item, true).returns(entity)

    buffer.delete(item_to_delete(:item_id, id))
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

    buffer.delete(item_to_delete(:item_id, id))
    buffer.delete(item_to_delete(:item_id, id2))
  end

  def test_delete_cannot_have_same_entity_as_both_top_level_and_dependent
    buffer = CouchTap::QueryBuffer.new

    buffer.delete(item_to_delete(:item_id, 123))
    assert_raises ArgumentError do
      buffer.delete(item_to_delete(:item_id, 987, false))
    end
  end

  def test_delete_increases_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.delete(item_to_delete(:item_id, 123))
    assert_equal 2, buffer.delete(item_to_delete(:item_id, 987))
  end

  def test_clear_deletes_newest_updated_at
    buffer = CouchTap::QueryBuffer.new
    dummy_date = Time.now.round
    assert_equal 1, buffer.insert(item_to_insert(true, :item_id, 123, { updated_at: dummy_date.rfc2822 }))
    assert_equal dummy_date, buffer.newest_updated_at
    assert_equal 2, buffer.delete(item_to_delete(:item_id, 987))
    buffer.clear
    assert_equal nil, buffer.newest_updated_at
  end

  def test_clear_resets_the_size
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(item_to_insert(true, :item_id, 123))
    assert_equal 2, buffer.delete(item_to_delete(:item_id, 987))
    buffer.clear
    assert_equal 1, buffer.insert(item_to_insert(true, :item_id, 123))
  end

  def test_clear_clears_the_buffer
    buffer = CouchTap::QueryBuffer.new

    assert_equal 1, buffer.insert(item_to_insert(true, :item_id, 123))
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

    assert_equal 1, buffer.insert(item_to_insert(true, :item_id, 123))
    assert_equal 2, buffer.delete(CouchTap::Operations::DeleteOperation.new(:another, true, :another_id, 987))

    entity_names = []
    buffer.each { |e| entity_names << e.name }
    assert_equal %i(item another), entity_names
  end

  def test_newest_updated_at
    dummy_date = Time.new(2008,6,21, 13,30,0)
    buffer = CouchTap::QueryBuffer.new

    buffer.insert(item_to_insert(true, :item_id, 123, { updated_at: dummy_date.rfc2822 }))
    assert_equal dummy_date, buffer.newest_updated_at

    buffer.insert(item_to_insert(true, :item_id, 456))
    assert_equal dummy_date, buffer.newest_updated_at

    buffer.insert(item_to_insert(true, :item_id, 123, { updated_at: (dummy_date - 10).rfc2822 }))
    assert_equal dummy_date, buffer.newest_updated_at

    buffer.insert(item_to_insert(true, :item_id, 123, { updated_at: (dummy_date + 10).rfc2822 }))
    assert_equal (dummy_date + 10), buffer.newest_updated_at
  end

  def test_top_level_items_are_overwritten
    buffer = CouchTap::QueryBuffer.new

    buffer.insert(item_to_insert(true, :item_id, 123, { a: 3, b: 5 }))
    buffer.insert(item_to_insert(true, :item_id, 123, { a: 'a', b: 6 }))
    entities = []
    buffer.each { |e| entities << e }

    assert_equal 1, entities.count
    assert_equal [['a', 6]], entities.first.insert_values(%i(a b))
  end

  def test_child_elements_are_deleted
    buffer = CouchTap::QueryBuffer.new

    buffer.insert(item_to_insert(false, :item_id, 123, { a: 3, b: 5 }))
    buffer.delete(item_to_delete(:item_id, 123, false))

    entities = []
    buffer.each { |e| entities << e }

    assert_equal 1, entities.count
    refute entities.first.any_insert?
  end

  private

  def item_to_insert(top_level, pk, id, data = nil)
    data ||= { item_id: id, name: 'dummy', count: rand() }
    CouchTap::Operations::InsertOperation.new(:item, top_level, pk, id, data)
  end

  def item_to_delete(pk, id, top_level=true, type=:item)
    CouchTap::Operations::DeleteOperation.new(type, top_level, pk, id)
  end
end
