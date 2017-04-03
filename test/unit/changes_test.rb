
require 'test_helper'

class ChangesTest < Test::Unit::TestCase

  def setup
    reset_test_db!
    build_sample_config
    @executor = @changes.instance_variable_get(:@query_executor)
    @queue = @changes.instance_variable_get(:@operations_queue)
  end

  def test_basic_init
    @database = @changes.database
    assert @changes.database, "Did not assign a database"
    assert @changes.database.is_a?(Sequel::Database)
    row = @database[:couch_sequence].first
    assert row, "Did not create a couch_sequence table"
    assert_equal row[:seq], 0, "Did not set a default sequence number"
    assert_equal row[:name], TEST_DB_NAME, "Sequence name does not match"
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

    handler = @changes.handlers.first
    handler.expects(:delete).with(doc, @queue)
    handler.expects(:insert).with(doc, @queue)

    @changes.send(:process_row, row)

    # Should update seq
    assert_equal @changes.seq, 1
  end

  def test_inserting_rows_with_multiple_filters
    doc = {'_id' => '1234', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}
    row = {'seq' => 3, 'id' => '1234', 'doc' => doc}

    handler = @changes.handlers[0]
    handler.expects(:insert).never
    handler = @changes.handlers[1]
    handler.expects(:delete)
    handler.expects(:insert)
    handler = @changes.handlers[2]
    handler.expects(:delete)
    handler.expects(:insert)

    @changes.send(:process_row, row)
    assert_equal @changes.seq, 3
  end

  def test_deleting_rows
    row = {'seq' => 9, 'id' => '1234', 'deleted' => true}

    @changes.handlers.each do |handler|
      handler.expects(:delete).with({'_id' => row['id']}, @queue)
    end

    @changes.send(:process_row, row)

    assert_equal @changes.seq, 9
  end

  def test_returning_schema
    schema = mock()
    CouchTap::Schema.expects(:new).once.with(@changes.database, :items).returns(schema)
    # Run twice to ensure cached
    assert_equal @changes.schema(:items), schema
    assert_equal @changes.schema(:items), schema
  end

  protected

  def build_sample_config
    @changes = CouchTap::Changes.new(TEST_DB_ROOT) do
      database db: "sqlite:/"
      document :type => 'Foo' do
      end
      document :type => 'Bar' do
      end
      document :type => 'Bar', :special => true do
      end
    end
  end

end
