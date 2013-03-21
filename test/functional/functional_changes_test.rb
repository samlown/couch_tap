require '../test_helper'

class FunctionalChangesTest < Test::Unit::TestCase

  def setup
    # Create a new CouchDB
    @source = CouchRest.database('couch_tap')
    create_sample_documents

    # Create a new Sqlite DB in memory
    @database = Sequel.sqlite
    migrate_sample_database
  end

  def test_something
    assert_equal "foo", "bar"
  end


  protected

  def migrate_sample_database
    @database.create_table :items do
      primary_key :id
      String :name
      Float :price
      Time :created_at
    end
  end

  def create_sample_documents
    @source.save_doc {:name => "Item 1", :price => 1.23, :created_at => Time.now}
    @source.save_doc {:name => "Item 2", :price => 2.23, :created_at => Time.now}
    @source.save_doc {:name => "Item 3", :price => 3.23, :created_at => Time.now}
  end

end
