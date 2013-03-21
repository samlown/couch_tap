
require '../test_helper'

class ChangesTest < Test::Unit::TestCase

  def test_basic_init
    @changes = CouchTap::Changes.new("couch_tap") do
      database "sqlite:/"
    end
    @database = @changes.database
    assert @changes.database, "Did not assign a database"
    assert @changes.database.is_a?(Sequel::Database)
    row = @database[:couch_sequence].first
    assert row, "Did not create a couch_sequence table"
    assert_equal row[:seq], '0', "Did not set a default sequence number"
  end


end
