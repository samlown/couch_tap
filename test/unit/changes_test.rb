
require 'test_helper'

class ChangesTest < Test::Unit::TestCase

  DB_TEST_FILE = "test.db"
  CHANGE_TESTS_SQLITE_DB = "sqlite://#{DB_TEST_FILE}"

  def setup
    reset_test_db!
    create_sqlite_file
    build_sample_config
  end

  def teardown
    remove_sqlite_file
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

  def test_sequential_initialization_for_more_than_one_source
    secondary_database_name = "secondary_database"
    secondary_database_root = File.join(TEST_DB_HOST, secondary_database_name)
    secondary_database = CouchRest.database(secondary_database_root)
    secondary_database.recreate!
    secondary_changes = CouchTap::Changes.new(secondary_database_root) do
      database CHANGE_TESTS_SQLITE_DB
    end
    
    secondary_database = secondary_changes.database[:couch_sequence].where(:name => secondary_database_name).first
    assert_equal secondary_database[:seq], 0, "Did not set a default sequence number for the secondary database"
    test_database = @changes.database[:couch_sequence].where(:name => TEST_DB_NAME).first
    assert_equal test_database[:seq], 0, "Did not set a default sequence number for the test database"
  end

  def test_defining_document_handler
    assert_equal @changes.handlers.length, 3
    handler = @changes.handlers.first
    assert handler.is_a?(CouchTap::DocumentHandler)
    assert_equal handler.filter, :type => 'Foo'
  end

  def test_inserting_rows
    row = {'seq' => 1, 'id' => '1234'}
    doc = {'_id' => '1234', 'type' => 'Foo', 'name' => 'Some Document'}
    @changes.expects(:fetch_document).with('1234').returns(doc)

    handler = @changes.handlers.first
    handler.expects(:delete).with(doc)
    handler.expects(:insert).with(doc)

    @changes.send(:process_row, row)

    # Should update seq
    assert_equal @changes.database[:couch_sequence].first[:seq], 1
  end

  def test_inserting_rows_with_mutiple_filters
    row = {'seq' => 3, 'id' => '1234'}
    doc = {'_id' => '1234', 'type' => 'Bar', 'special' => true, 'name' => 'Some Document'}
    @changes.expects(:fetch_document).with('1234').returns(doc)

    handler = @changes.handlers[0]
    handler.expects(:insert).never
    handler = @changes.handlers[1]
    handler.expects(:delete)
    handler.expects(:insert)
    handler = @changes.handlers[2]
    handler.expects(:delete)
    handler.expects(:insert)

    @changes.send(:process_row, row)
    assert_equal @changes.database[:couch_sequence].first[:seq], 3
  end

  def test_deleting_rows
    row = {'seq' => 9, 'id' => '1234', 'deleted' => true}

    @changes.handlers.each do |handler|
      handler.expects(:delete).with({'_id' => row['id']})
    end

    @changes.send(:process_row, row)

    assert_equal @changes.database[:couch_sequence].first[:seq], 9
  end

  def test_returning_schema
    schema = mock()
    CouchTap::Schema.expects(:new).once.with(@changes.database, :items).returns(schema)
    # Run twice to ensure cached
    assert_equal @changes.schema(:items), schema
    assert_equal @changes.schema(:items), schema
  end

  protected

  def create_sqlite_file
    File.open(DB_TEST_FILE, "w") {}
  end

  def remove_sqlite_file
    File.delete(DB_TEST_FILE) if File.exists?(DB_TEST_FILE)
  end

  def build_sample_config
    @changes = CouchTap::Changes.new(TEST_DB_ROOT) do
      database CHANGE_TESTS_SQLITE_DB
      document :type => 'Foo' do
      end
      document :type => 'Bar' do
      end
      document :type => 'Bar', :special => true do
      end
    end
  end

end
