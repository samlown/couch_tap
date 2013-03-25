
require 'test_helper'

class ChangesTest < Test::Unit::TestCase

  def setup
    reset_test_db!
  end

  def test_basic_init
    @changes = CouchTap::Changes.new(TEST_DB_ROOT) do
      database "sqlite:/"
    end
    @database = @changes.database
    assert @changes.database, "Did not assign a database"
    assert @changes.database.is_a?(Sequel::Database)
    row = @database[:couch_sequence].first
    assert row, "Did not create a couch_sequence table"
    assert_equal row[:seq], 0, "Did not set a default sequence number"
    assert_equal row[:name], TEST_DB_NAME, "Sequence name does not match"
  end

  def test_defining_document_handler
    @changes = CouchTap::Changes.new(TEST_DB_ROOT) do
      database "sqlite:/"
      document :type => 'Foo' do
        # Nothing
      end
    end
    assert_equal @changes.handlers.length, 1
    handler = @changes.handlers.first
    assert_equal handler[0], :type => 'Foo'
    assert handler[1].is_a?(CouchTap::DocumentHandler)
  end


  def test_processing_rows_adding
    @changes = CouchTap::Changes.new(TEST_DB_ROOT) do
      database "sqlite:/"
      document :type => 'Foo' do
        # Nothing
      end
    end
    row = {'seq' => 1, 'id' => '1234'}
    doc = {'_id' => '1234', 'type' => 'Foo', 'name' => 'Some Document'}

    @changes.expects(:fetch_document).with('1234').returns(doc)

    handler = @changes.handlers.first
    handler[1].expects(:execute).with(doc)

    @changes.send(:process_row, row)

    # Should update seq
    assert_equal @changes.database[:couch_sequence].first[:seq], 1
  end

end
