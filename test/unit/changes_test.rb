
require 'test_helper'

class ChangesTest < Test::Unit::TestCase

  def setup
    reset_test_db!
    build_sample_config
    @executor = @changes.instance_variable_get(:@query_executor)
    initialize_database @executor.database
    @queue = @changes.instance_variable_get(:@operations_queue)
  end

  def test_defining_document_handler
    assert_equal @changes.handlers.length, 3
    handler = @changes.handlers.first
    assert handler.is_a?(CouchTap::DocumentHandler)
    assert_equal handler.filter, :type => 'Foo'
  end

  def test_inserting_rows
    doc = {'_id' => '1234', 'type' => 'Foo', 'name' => 'Some Document'}
    row = {'seq' => 1, 'id' => '1234', 'doc' => doc}

    @changes.send(:process_row, row)

    assert_equal 4, @queue.length
    assert_equal CouchTap::Operations::BeginTransactionOperation.new, @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:foo, true, :foo_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::InsertOperation.new(:foo, true, '1234', foo_id: '1234', name: 'Some Document'), @queue.pop
    assert_equal CouchTap::Operations::EndTransactionOperation.new(1), @queue.pop
  end

  def test_inserting_rows_with_multiple_filters
    doc = {'_id' => '1234', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}
    row = {'seq' => 3, 'id' => '1234', 'doc' => doc}

    @changes.send(:process_row, row)

    assert_equal 6, @queue.length
    assert_equal CouchTap::Operations::BeginTransactionOperation.new, @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:bar, true, :bar_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::InsertOperation.new(:bar, true, '1234', bar_id: '1234', name: 'Some Document'), @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:special_bar, true, :special_bar_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::InsertOperation.new(:special_bar, true, '1234', special_bar_id: '1234', name: 'Some Document', special: true), @queue.pop
    assert_equal CouchTap::Operations::EndTransactionOperation.new(3), @queue.pop
  end

  def test_deleting_rows
    row = {'seq' => 9, 'id' => '1234', 'deleted' => true}

    @changes.send(:process_row, row)

    assert_equal 5, @queue.length
    assert_equal CouchTap::Operations::BeginTransactionOperation.new, @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:foo, true, :foo_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:bar, true, :bar_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:special_bar, true, :special_bar_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::EndTransactionOperation.new(9), @queue.pop
  end

  def test_returning_schema
    schema = mock()
    CouchTap::Schema.expects(:new).once.with(@executor.database, :items).returns(schema)
    # Run twice to ensure cached
    assert_equal @changes.schema(:items), schema
    assert_equal @changes.schema(:items), schema
  end

# TODO test the perform_request method too!!

  def test_timer_signal
    @changes.instance_variable_set(:@timeout, 0.1)
    @changes.send(:start_timer)
    sleep 0.2
    @changes.stop_timer

    assert_equal 1, @queue.length
    assert @queue.pop.is_a? CouchTap::Operations::TimerFiredSignal
  end

  protected

  def build_sample_config
    @changes = CouchTap::Changes.new couch_db: TEST_DB_ROOT, timeout: 60 do
      database db: "sqlite:/"
      document :type => 'Foo' do
        table :foo do
        end
      end
      document :type => 'Bar' do
        table :bar do
        end
      end
      document :type => 'Bar', :special => true do
        table :special_bar do
        end
      end
    end
  end

  def initialize_database(connection)
    connection.create_table :foo do
      String :name
      Boolean :special
    end

    connection.create_table :bar do
      String :name
    end

    connection.create_table :special_bar do
      String :name
      Boolean :special
    end
  end
end
