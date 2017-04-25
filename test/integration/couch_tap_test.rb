
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

  def test_insert_update_delete
    config = <<-CFG
      changes couch_db: TEST_DB_ROOT, timeout: 0.5 do
        database db: "sqlite://#{DB_FILE}", batch_size: 10

        document type: "Sale" do
          table :sales do
            column :audited_at, Time.now
            collection :entries do
              table :sale_entries do
                column :audited_at, Time.now
              end
            end
          end
        end

        document type: "AnalyticEvent" do
          table :analytic_events do
          end
        end
      end
    CFG

    CouchTap.module_eval(config)
    th = Thread.new do
      CouchTap.start
    end
    th.abort_on_exception = true

    TEST_DB.save_doc(type: "Sale", code: "R", amount: 20, entries: [ { price: 10.0 }, { price: 20.0 }, { price: 30.0 }, { price: 40.0 }, { price: 50.0 }, { price: 60.0 }, { price: 70.0 }])

    sleep 0.1

    assert_equal 1, @db[:sales].count
    sale = @db[:sales].first
    assert_equal "R", sale[:code]
    assert_equal 20, sale[:amount]
    entries = @db[:sale_entries].where(sale_id: sale[:sale_id]).to_a
    assert_equal 7, entries.count
    (10..70).step(10) { |price| assert_includes entries, price: price, sale_id: sale[:sale_id] }
    assert_equal 1, @db[:couch_sequence].where(name: TEST_DB_NAME).first[:seq]

    docu = TEST_DB.update_doc sale[:sale_id] do |doc|
      doc[:amount] = 200
      doc[:entries] = [{ price: 100.0 }, { price: 200.0 }, { price: 300.0 }, { price: 400.0 }, { price: 500.0 }, { price: 600.0 }, { price: 700.0 }]
    end

    sleep 0.1

    sale_bak = sale
    assert_equal 1, @db[:sales].count
    sale = @db[:sales].first
    assert_equal sale_bak[:sale_id], sale[:sale_id]
    assert_equal "R", sale[:code]
    assert_equal 200, sale[:amount]
    entries = @db[:sale_entries].where(sale_id: sale[:sale_id]).to_a
    assert_equal 7, entries.count
    (100..700).step(100) { |price| assert_includes entries, price: price, sale_id: sale[:sale_id] }
    assert_equal 2, @db[:couch_sequence].where(name: TEST_DB_NAME).first[:seq]

    TEST_DB.delete_doc(docu)

    sleep 1 # Sleep to allow the timer to run

    assert_equal 0, @db[:sales].count

    CouchTap.stop

    TEST_DB.save_doc(dummy: true) # This is a HACK to make the client disconnect from the feed.

    th.join

    assert_equal 3, @db[:couch_sequence].where(name: TEST_DB_NAME).first[:seq]
  end

  private

  def setup_db(connection)
    connection.create_table :sales do
      String :sale_id
      String :code
      Float :amount
      Time :updated_at
    end

    connection.create_table :sale_entries do
      String :sale_id
      Float :price
    end

    connection.create_table :analytic_events do
      String :analytic_event_id
      String :key
      String :value
    end
  end
end
