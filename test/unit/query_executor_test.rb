
require 'test_helper'

class QueryExecutorTest < Test::Unit::TestCase

  def test_insert_saves_the_data_if_not_full
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    executor.row 1 do
      executor.insert(:items, true, 123, item_id: 123, name: 'dummy')
    end

    assert_equal 0, executor.database[:items].count
  end

  def test_cannot_insert_outside_a_row_document
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    assert_raises RuntimeError do
      executor.insert(:items, true, 123, item_id: 123, name: 'dummy')
    end

    assert_equal 0, executor.database[:items].count
  end

  def test_insert_runs_the_query_if_full
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 1 do
      executor.insert(:items, true, 123, item_id: 123, name: 'dummy')
      executor.insert(:items, true, 987, item_id: 987, name: 'dummy')
    end

    assert_equal 2, executor.database[:items].count
  end

  def test_insert_fails_rollsback_the_transaction
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    assert_raises Sequel::UniqueConstraintViolation do
      executor.row 1 do
        executor.insert(:items, false, 123, item_id: 123, name: 'dummy')
        executor.insert(:items, false, 123, item_id: 123, name: 'dummy')
      end
    end

    assert_equal 0, executor.database[:items].count
    assert_equal 0, executor.database[:couch_sequence].where(name: 'items').first[:seq]
  end

  def test_delete_saves_the_data_if_not_full
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    id = 123

    executor.database[:items].insert(item_id: id, name: 'dummy')
    assert_equal 1, executor.database[:items].count

    executor.row 1 do
      executor.delete(:items, true, item_id: id)
    end

    assert_equal 1, executor.database[:items].count
  end

  def test_delete_runs_the_query_if_full
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 1
    initialize_database executor.database

    id = 123

    executor.database[:items].insert(item_id: id, name: 'dummy')
    assert_equal 1, executor.database[:items].count

    executor.row 1 do
      executor.delete(:items, true, item_id: id)
    end

    assert_equal 0, executor.database[:items].count
  end

  def test_delete_fails_rollsback_the_transaction
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    assert_raises Sequel::DatabaseError do
      executor.row 1 do
        executor.delete(:items, true, item_id: 123)
        executor.delete(:cow, true, cow_id: 234)
      end
    end

    assert_equal 0, executor.database[:items].count
    assert_equal 0, executor.database[:couch_sequence].where(name: 'items').first[:seq]
  end

  def test_create_and_delete_in_same_row
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 1 do
      executor.insert(:items, true, 123, item_id: 123, a: 1, b: 'b')
      executor.delete(:items, true, item_id: 123)
    end

    assert_equal 0, executor.database[:items].where(item_id: 123).count
  end

  def test_includes_whole_row_even_if_batch_gets_oversized
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 1 do
      executor.insert(:items, true, 123, item_id: 123, count: 1, name: 'b')
      executor.insert(:items, true, 234, item_id: 234, count: 2, name: 'c')
      executor.insert(:items, true, 345, item_id: 345, count: 3, name: 'd')
      executor.insert(:items, true, 456, item_id: 456, count: 4, name: 'e')
    end

    assert_equal 4, executor.database[:items].count
  end

  def test_combined_workload
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 3
    initialize_database executor.database

    executor.row 1 do
      # Create and destroy item 123
      executor.insert(:items, true, 123, item_id: 123, count: 1, name: 'b')
      executor.delete(:items, true, item_id: 123)

      # Insert items 234, 345 and 456
      executor.insert(:items, true, 234, item_id: 234, count: 2, name: 'c')
      executor.insert(:items, true, 345, item_id: 345, count: 3, name: 'd')
      executor.insert(:items, true, 456, item_id: 456, count: 4, name: 'e')
    end

    executor.row 2 do
      # Update item 234
      executor.delete(:items, true, item_id: 234)
      executor.insert(:items, true, 234, item_id: 234, count: 4, name: 'new')

      # Delete item 345
      executor.delete(:items, true, item_id: 345)
    end

    expected = [
      { item_id: "456", name: "e", count: 4, price: nil, created_at: nil }, 
      { item_id: "234", name: "new", count: 4, price: nil, created_at: nil }
    ]

    assert_equal expected, executor.database[:items].to_a
  end

  def test_cannot_delete_outside_a_row_document
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    id = 123

    executor.database[:items].insert(item_id: id, name: 'dummy')
    assert_equal 1, executor.database[:items].count

    assert_raises RuntimeError do
      executor.delete(:items, true, item_id: 123)
    end

    assert_equal 1, executor.database[:items].count
  end

  def test_sequence_number_defaults_to_zero
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 10
    initialize_database executor.database
    assert_equal 0, executor.seq
  end

  def test_sequence_number_is_loaded_on_initialization
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite://test.db', batch_size: 10
    initialize_database executor.database
    executor.database[:couch_sequence].where(name: 'items').update(seq: 432)

    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite://test.db', batch_size: 10
    assert_equal 432, executor.seq

    File.delete('test.db')
  end

  def test_sequence_number_is_kept_in_memory_if_no_transaction
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    executor.row 500 do
      executor.insert(:items, true, 123, item_id: 123, name: 'n')
    end

    assert_equal 500, executor.seq
    assert_equal 0, executor.database[:couch_sequence].where(name: 'items').first[:seq]
  end

  def test_sequence_number_is_saved_upon_transaction
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 1
    initialize_database executor.database

    executor.row 500 do
      executor.insert(:items, true, 123, item_id: 123, name: 'n')
    end

    assert_equal 500, executor.seq
    assert_equal 500, executor.database[:couch_sequence].where(name: 'items').first[:seq]
  end

  private

  def initialize_database(connection)
    connection.create_table :items do
      String :item_id
      String :name
      Integer :count
      Float :price
      Time :created_at
      index :item_id, :unique => true
    end
    connection
  end
end
