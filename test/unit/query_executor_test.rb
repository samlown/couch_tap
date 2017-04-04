
require 'test_helper'

class QueryExecutorTest < Test::Unit::TestCase

  def test_insert_saves_the_data_if_not_full
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    executor.row 1 do
      queue.add_operation(item_to_insert(true, 123))
    end

    assert_equal 0, executor.database[:items].count
  end

  def test_insert_runs_the_query_if_full
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 1 do
      queue.add_operation(item_to_insert(true, 123))
      queue.add_operation(item_to_insert(true, 987))
    end

    assert_equal 2, executor.database[:items].count
  end

  def test_insert_fails_rollsback_the_transaction
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    assert_raises Sequel::UniqueConstraintViolation do
      executor.row 1 do
        queue.add_operation(item_to_insert(false, 123))
        queue.add_operation(item_to_insert(false, 123))
      end
    end

    assert_equal 0, executor.database[:items].count
    assert_equal 0, executor.database[:couch_sequence].where(name: 'items').first[:seq]
  end

  def test_delete_saves_the_data_if_not_full
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    id = 123

    executor.database[:items].insert(item_id: id, name: 'dummy')
    assert_equal 1, executor.database[:items].count

    executor.row 1 do
      queue.add_operation(item_to_delete(id))
    end

    assert_equal 1, executor.database[:items].count
  end

  def test_delete_runs_the_query_if_full
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 1
    initialize_database executor.database

    id = 123

    executor.database[:items].insert(item_id: id, name: 'dummy')
    assert_equal 1, executor.database[:items].count

    executor.row 1 do
      queue.add_operation(item_to_delete(id))
    end

    assert_equal 0, executor.database[:items].count
  end

  def test_delete_fails_rollsback_the_transaction
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 0 do
      executor.insert(:items, false, 456, item_id: 456, name: 'dummy')
      executor.insert(:items, false, 789, item_id: 789, name: 'dummy')
    end

    assert_raises Sequel::DatabaseError do
      executor.row 1 do
        queue.add_operation(item_to_delete(123))
        queue.add_operation(CouchTap::Operations::DeleteOperation.new(:cow, true, :cow_id, 234))
      end
    end

    assert_equal 2, executor.database[:items].count
    assert_equal 0, executor.database[:couch_sequence].where(name: 'items').first[:seq]
  end

  def test_create_and_delete_in_same_row
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 1 do
      queue.add_operation(item_to_insert(true, 123))
      queue.add_operation(item_to_delete(123))
    end

    assert_equal 0, executor.database[:items].where(item_id: 123).count
  end

  def test_includes_whole_row_even_if_batch_gets_oversized
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 1 do
      queue.add_operation(item_to_insert(true, 123))
      queue.add_operation(item_to_insert(true, 234))
      queue.add_operation(item_to_insert(true, 345))
      queue.add_operation(item_to_insert(true, 456))
    end

    assert_equal 4, executor.database[:items].count
  end

  def test_prepends_a_begin_transaction_operation
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.row 1 do
    end

    assert_true queue.pop.is_a? CouchTap::Operations::BeginTransactionOperation
  end

  def test_combined_workload
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 3
    initialize_database executor.database

    executor.row 1 do
      # Create and destroy item 123
      queue.add_operation(item_to_insert(true, 123))
      queue.add_operation(item_to_delete(123))

      # Insert item_to_inserts 234, 345 and 456
      queue.add_operation(item_to_insert(true, 234))
      queue.add_operation(item_to_insert(true, 345))
      queue.add_operation(item_to_insert(true, 456))
    end

    executor.row 2 do
      # Update item 234
      queue.add_operation(item_to_delete(234))
      queue.add_operation(item_to_insert(true, 234))

      # Delete item 345
      queue.add_operation(item_to_delete(345))
    end

    assert_equal %w(234 456), executor.database[:items].select(:item_id).to_a.map { |i| i[:item_id] }
  end

  def test_delete_nested_items
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 2
    initialize_database executor.database

    executor.database.create_table :item_children do
      String :item_id
      String :child_name
    end

    executor.row 1 do
      executor.insert(:items, true, 123, item_id: 123, count: 1, name: 'name')
      executor.insert(:item_children, false, 123, item_id: 123, child_name: 'child name')
    end

    executor.row 2 do
      executor.delete(:items, true, item_id: 123)
      executor.insert(:items, true, 123, item_id: 123, count: 2, name: 'another name')
      executor.delete(:item_children, false, item_id: 123)
      executor.insert(:item_children, false, 123, item_id: 123, child_name: 'another child name')
    end
    assert_equal 2, executor.database[:items].first[:count]
    assert_equal 'another child name', executor.database[:item_children].first[:child_name]
  end

  def test_cannot_delete_outside_a_row_document
    executor = CouchTap::QueryExecutor.new 'items', db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    id = 123

    executor.database[:items].insert(item_id: id, name: 'dummy')
    assert_equal 1, executor.database[:items].count

    assert_raises RuntimeError do
      executor.delete(item_to_delete(123))
    end

    assert_equal 1, executor.database[:items].count
  end

  def test_sequence_number_defaults_to_zero
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 10
    initialize_database executor.database
    assert_equal 0, executor.seq
  end

  def test_sequence_number_is_loaded_on_initialization
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite://test.db', batch_size: 10
    initialize_database executor.database
    executor.database[:couch_sequence].where(name: 'items').update(seq: 432)

    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite://test.db', batch_size: 10
    assert_equal 432, executor.seq

    File.delete('test.db')
  end

  def test_sequence_number_is_kept_in_memory_if_no_transaction
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 10
    initialize_database executor.database

    executor.row 500 do
      queue.add_operation(item_to_insert(true, 123))
    end

    assert_equal 500, executor.seq
    assert_equal 0, executor.database[:couch_sequence].where(name: 'items').first[:seq]
  end

  def test_sequence_number_is_saved_upon_transaction
    queue = CouchTap::OperationsQueue.new
    executor = CouchTap::QueryExecutor.new 'items', queue, db: 'sqlite:/', batch_size: 1
    initialize_database executor.database

    executor.row 500 do
      queue.add_operation(item_to_insert(true, 123))
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

  def item_to_insert(top_level, id)
    CouchTap::Operations::InsertOperation.new(:items, top_level, id, item_id: id, name: 'dummy', count: rand())
  end

  def item_to_delete(id)
    CouchTap::Operations::DeleteOperation.new(:items, true, :item_id, id)
  end
end
