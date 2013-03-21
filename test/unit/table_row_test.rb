require '../test_helper'

class TableRowTest < Test::Unit::TestCase

  def setup
    @handler = MiniTest::Mock.new
  end

  def test_something
    assert_equal "foo", "bar"
  end


end
