require 'test_helper'

class CouchTapIntegrationTest < Test::Unit::TestCase
  DB_FILE = "integration.sqlite"
  DUMMY_ANALYTIC_EVENT = { type: "AnalyticEvent", key: "click", value: "6j26n146" }
  DUMMY_SALE = { type: "Sale",
                code: "R", amount: 20, entries: [ { price: 10.0 },
                                                  { price: 20.0 },
                                                  { price: 30.0 },
                                                  { price: 40.0 },
                                                  { price: 50.0 },
                                                  { price: 60.0 },
                                                  { price: 70.0 }]}
  DUMMY_UPDATE_FIELDS = { amount: 200, entries: (1..7).map { |i| { price: 100.0 * i } } }

  def setup
    reset_test_db!
    WebMock.allow_net_connect!
    CouchTap::Changes.send(:remove_const, "RECONNECT_TIMEOUT")
    CouchTap::Changes.const_set("RECONNECT_TIMEOUT", 0.1)
    @db = Sequel.connect("sqlite://#{DB_FILE}?mode=rwc")
    setup_db @db
  end

  def teardown
    CouchTap.instance_variable_set(:@changes, nil)
    @db.disconnect
    File.delete(DB_FILE)
  end

  def test_insert_update_delete
    CouchTap.module_eval(config)
    th = Thread.new { CouchTap.start }
    th.abort_on_exception = true

    sale_id = insert_sale_with_entries
    sale = update_sale_with_entries(sale_id)
    delete_sale_and_insert_analytic_event(sale)

    CouchTap.stop
    TEST_DB.save_doc(dummy: true) # This is a HACK to make the client disconnect from the feed.
    th.join
  end

  def test_reprocess_and_go_live
    15.times do |i|
      TEST_DB.save_doc(DUMMY_ANALYTIC_EVENT.merge(value: i))
    end

    CouchTap.module_eval(config)
    th = Thread.new { CouchTap.start }
    th.abort_on_exception = true

    sleep 1
    assert_equal 15, @db[:analytic_events].count

    CouchTap.stop
    TEST_DB.save_doc(dummy: true) # This is a HACK to make the client disconnect from the feed.
    th.join
  end

  private

  def delete_sale_and_insert_analytic_event(sale)
    TEST_DB.delete_doc(sale)
    TEST_DB.save_doc(DUMMY_ANALYTIC_EVENT.merge(value: Time.now))

    sleep 1 # Sleep to allow the timer to run

    assert_equal 0, @db[:sales].count
    assert_equal 1, @db[:analytic_events].count
    assert_equal 4, @db[:couch_sequence].where(name: TEST_DB_NAME).first[:seq]
  end

  def insert_sale_with_entries
    Timecop.freeze(Time.now.round - 120)
    TEST_DB.save_doc(DUMMY_SALE)
    sleep 1
    assert_equal 1, @db[:sales].count
    assert_equal 1, @db[:couch_sequence].where(name: TEST_DB_NAME).first[:seq]
    sale = @db[:sales].first
    assert_equal_sale(DUMMY_SALE, sale)
    Timecop.return
    sale[:sale_id]
  end

  def update_sale_with_entries(sale_id)
    Timecop.freeze(Time.now)
    docu = TEST_DB.update_doc sale_id do |doc|
      doc[:amount] = DUMMY_UPDATE_FIELDS[:amount]
      doc[:entries] = DUMMY_UPDATE_FIELDS[:entries]
    end
    sleep 0.1
    assert_equal 1, @db[:sales].count
    assert_equal 2, @db[:couch_sequence].where(name: TEST_DB_NAME).first[:seq]
    expected_updated_sale = DUMMY_SALE.merge(DUMMY_UPDATE_FIELDS)
    updated_sale = @db[:sales].first
    assert_equal sale_id, updated_sale[:sale_id]
    assert_equal_sale expected_updated_sale, updated_sale
    Timecop.return
    docu
  end


  def assert_equal_sale(expected_sale, db_sale)
    assert_equal expected_sale[:code], db_sale[:code]
    assert_equal expected_sale[:amount], db_sale[:amount]
    assert_equal Time.now, db_sale[:audited_at]
    entries = @db[:sale_entries].where(sale_id: db_sale[:sale_id]).to_a
    assert_equal expected_sale[:entries].size, entries.count
    expected_sale[:entries].each { |entry| assert_includes entries, price: entry[:price], sale_id: db_sale[:sale_id] }
  end

  def config
    <<-CFG
      changes couch_db: TEST_DB_ROOT, timeout: 0.5 do
        database db: "sqlite://#{DB_FILE}?mode=rw", batch_size: 10

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
  end

  def setup_db(connection)
    connection.create_table :sales do
      String :sale_id
      String :code
      Float :amount
      Time :updated_at
      Time :audited_at
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
