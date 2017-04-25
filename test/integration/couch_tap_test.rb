
require 'test_helper'

class CouchTapIntegrationTest < Test::Unit::TestCase
  DB_FILE = "integration.sqlite"

  def setup
    reset_test_db!
    WebMock.allow_net_connect!
    CouchTap::Changes.send(:remove_const, "RECONNECT_TIMEOUT")
    CouchTap::Changes.const_set("RECONNECT_TIMEOUT", 0.1)
    @db = Sequel.connect("sqlite://#{DB_FILE}")
    setup_db @db
  end

  def teardown
    File.delete(DB_FILE)
  end

  def test_insert
    config = <<-EOF
      changes couch_db: TEST_DB_ROOT, timeout: 1 do
        database db: "sqlite://integration.sqlite", batch_size: 10_000

        document type: "Foo" do
          table :foo do

          end
        end
      end
    EOF

    CouchTap.module_eval(config)
    th = Thread.new do
      CouchTap.start
    end
    th.abort_on_exception = true

    TEST_DB.save_doc(type: "Foo", name: "The name", special: false)

    CouchTap.stop

    TEST_DB.save_doc(dummy: true)

    th.join

    assert_equal 1, @db[:foo].count
  end

  private

  def setup_db(connection)
    connection.create_table :foo do
      String :foo_id
      String :name
      Boolean :special
    end
  end
end
