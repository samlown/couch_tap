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
    assert_equal CouchTap::Operations::InsertOperation.new(:foo, true, :foo_id, '1234', foo_id: '1234', name: 'Some Document'), @queue.pop
    assert_equal CouchTap::Operations::EndTransactionOperation.new(1), @queue.pop
  end

  def test_inserting_rows_with_multiple_filters
    doc = {'_id' => '1234', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}
    row = {'seq' => 3, 'id' => '1234', 'doc' => doc}

    @changes.send(:process_row, row)

    assert_equal 6, @queue.length
    assert_equal CouchTap::Operations::BeginTransactionOperation.new, @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:bar, true, :bar_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::InsertOperation.new(:bar, true, :bar_id, '1234', bar_id: '1234', name: 'Some Document'), @queue.pop
    assert_equal CouchTap::Operations::DeleteOperation.new(:special_bar, true, :special_bar_id, '1234'), @queue.pop
    assert_equal CouchTap::Operations::InsertOperation.new(:special_bar, true, :special_bar_id, '1234', special_bar_id: '1234', name: 'Some Document', special: true), @queue.pop
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

  def test_timer_signal
    @changes.instance_variable_set(:@timeout, 0.1)
    @changes.send(:start_timer)
    sleep 0.2
    @changes.stop_timer

    assert_equal 1, @queue.length
    assert @queue.pop.is_a? CouchTap::Operations::TimerFiredSignal
  end

  def test_sequence_in_url_is_updated_on_retries
    @changes.class.send(:remove_const, "RECONNECT_TIMEOUT")
    @changes.class.const_set("RECONNECT_TIMEOUT", 0.1)

    stub_request(:get, "#{TEST_DB_ROOT}/_changes").
      with(query: { feed: 'normal', heartbeat: 30_000, include_docs: true, since: 0, limit: 50_000 }).
      to_return(status: 200, body: { results: [] }.to_json)

    doc = {'_id' => '1234', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}
    row = {'seq' => 3, 'id' => '1234', 'doc' => doc}

    initial_req = stub_request(:get, "#{TEST_DB_ROOT}/_changes").
      with(query: { feed: 'continuous', heartbeat: 30000, include_docs: true, since: 0 }).
      to_return(status: 200, body: row.to_json )

    updated_req = stub_request(:get, "#{TEST_DB_ROOT}/_changes").
      with(query: { feed: 'continuous', heartbeat: 30000, include_docs: true, since: row['seq'] }).
      to_return(status: 200, body: { "last_seq" => 50_000 }.to_json )

    @changes.send(:start_timer)
    @changes.send(:prepare_parser)
    @changes.send(:start_consumer)

    t = Thread.new do
      sleep 0.2
      @changes.stop
    end
    t.abort_on_exception = true

    @changes.send(:start_producer)
    t.join

    assert_requested initial_req
    assert_requested updated_req
  end

  def test_normal_to_continuous_transition
    @changes.class.send(:remove_const, "RECONNECT_TIMEOUT")
    @changes.class.const_set("RECONNECT_TIMEOUT", 0.1)
    @changes.instance_variable_set(:@batch_size, 2)

    rows= [
      {'seq' => 3, 'id' => '1234', 'doc' => {'_id' => '1234', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}},
      {'seq' => 4, 'id' => '2345', 'doc' => {'_id' => '2345', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}}
    ]

    initial_req = stub_request(:get, "#{TEST_DB_ROOT}/_changes").
      with(query: { feed: 'normal', heartbeat: 30_000, include_docs: true, since: 0, limit: 2 }).
      to_return(status: 200, body: { results: rows, last_seq: 4 }.to_json)

    updated_req = stub_request(:get, "#{TEST_DB_ROOT}/_changes").
      with(query: { feed: 'normal', heartbeat: 30_000, include_docs: true, since: 4, limit: 2 }).
      to_return(status: 200, body: { results: [] }.to_json)

    new_row = {'seq' => 5, 'id' => '3456', 'doc' => {'_id' => '2345', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}}

    continuous_req = stub_request(:get, "#{TEST_DB_ROOT}/_changes").
      with(query: { feed: 'continuous', heartbeat: 30000, include_docs: true, since: 4 }).
      to_return(status: 200, body: new_row.to_json )

    last_req = stub_request(:get, "#{TEST_DB_ROOT}/_changes").
      with(query: { feed: 'continuous', heartbeat: 30000, include_docs: true, since: 5 }).
      to_return(status: 200, body: {}.to_json )

    @changes.send(:start_timer)
    @changes.send(:prepare_parser)
    @changes.send(:start_consumer)

    @changes.send(:reprocess)
    assert_requested initial_req
    assert_requested updated_req

    t = Thread.new do
      sleep 0.2
      @changes.stop
    end
    t.abort_on_exception = true

    @changes.instance_variable_set(:@status, CouchTap::Changes::RUNNING)
    @changes.send(:live_processing)
    t.join

    assert_requested continuous_req
    assert_requested last_req
  end

  protected

  def build_sample_config
    stub_request(:get, TEST_DB_ROOT).
      with(headers: { 'Accept'=>'application/json', 'Content-Type'=>'application/json' }).
      to_return(status: 200, body: { db_name: TEST_DB_NAME }.to_json, headers: {})

    @changes = CouchTap::Changes.new couch_db: TEST_DB_ROOT, timeout: 0.1 do
      database db: "sqlite:/", batch_size: 50_000
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
      String :bar_id
      String :name
    end

    connection.create_table :special_bar do
      String :special_bar_id
      String :name
      Boolean :special
    end
  end
end
