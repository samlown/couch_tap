require 'test_helper'

class FunctionalChangesTest < Test::Unit::TestCase

  def setup
    stub_request(:get, TEST_DB_ROOT).
      with(headers: { 'Accept'=>'application/json', 'Content-Type'=>'application/json' }).
      to_return(status: 200, body: { db_name: TEST_DB_NAME }.to_json , headers: {})
  end

  def test_insert_sales
    doc = { "id" => 1, "seq" => 123, "doc" => {
      "_id" => "10", "type" => "Sale", "code" => "Code 1", "amount" => 600
    }}

    changes = config_changes batch_size: 2

    changes.send(:process_row, doc)

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 1, sales.count
    assert_equal({ sale_id: "10", code: "Code 1", amount: 600, updated_at: nil }, sales.first)
    assert_sequence 123
  end

  def test_insert_multiple_sales
    docs = [
      { "id" => 1, "seq" => 123, "doc" => {
        "_id" => "10", "type" => "Sale", "code" => "Code 1", "amount" => 600
      }},
      { "id" => 2, "seq" => 124, "doc" => {
        "_id" => "11", "type" => "Sale", "code" => "Code 2", "amount" => 1000
      }},
      { "id" => 3, "seq" => 125, "doc" => {
        "_id" => "12", "type" => "Sale", "code" => "Code 3", "amount" => 325
      }}
    ]

    changes = config_changes batch_size: 3

    docs.each { |d| changes.send(:process_row, d) }

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 3, sales.count
    assert_includes sales, sale_id: "10", code: "Code 1", amount: 600, updated_at: nil
    assert_includes sales, sale_id: "11", code: "Code 2", amount: 1000, updated_at: nil
    assert_includes sales, sale_id: "12", code: "Code 3", amount: 325, updated_at: nil
    assert_sequence 125
  end

  def test_insert_multiple_sales_in_same_batch
    example_time = Time.new(2008,6,21, 13,30,0)
    docs = [
      { "id" => 1, "seq" => 123, "doc" => {
        "_id" => "10", "type" => "Sale", "code" => "Code 1", "amount" => 600,
        "updated_at" => (example_time - 120).rfc2822
      }},
      { "id" => 2, "seq" => 124, "doc" => {
        "_id" => "11", "type" => "Sale", "code" => "Code 2", "amount" => 1000
      }},
      { "id" => 3, "seq" => 125, "doc" => {
        "_id" => "12", "type" => "Sale", "code" => "Code 3", "amount" => 325,
        "updated_at" => example_time.rfc2822
      }}
    ]

    changes = config_changes batch_size: 7

    docs.each { |d| changes.send(:process_row, d) }

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 3, sales.count
    assert_includes sales, sale_id: "10", code: "Code 1", amount: 600, updated_at: (example_time - 120)
    assert_includes sales, sale_id: "11", code: "Code 2", amount: 1000, updated_at: nil
    assert_includes sales, sale_id: "12", code: "Code 3", amount: 325, updated_at:  example_time
    assert_sequence 125
    assert_equal @database[:couch_sequence].where(name: TEST_DB_NAME).to_a.first[:last_transaction_at], example_time
  end

  def test_insert_and_update_sale_in_different_batch
    docs = [
      { "id" => 1, "seq" => 123, "doc" => {
        "_id" => "10", "type" => "Sale", "code" => "Code 1", "amount" => 600
      }},
      { "id" => 2, "seq" => 124, "doc" => {
        "_id" => "10", "type" => "Sale", "code" => "Code 2", "amount" => 800
      }}
    ]

    changes = config_changes batch_size: 2

    docs.each { |d| changes.send(:process_row, d) }

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 1, sales.count
    assert_equal({ sale_id: "10", code: "Code 2", amount: 800, updated_at: nil }, sales.first)
    assert_sequence 124
  end

  def test_insert_and_update_sale_in_same_batch
    docs = [
      { "id" => 1, "seq" => 123, "doc" => {
        "_id" => "10", "type" => "Sale", "code" => "Code 1", "amount" => 600
      }},
      { "id" => 2, "seq" => 124, "doc" => {
        "_id" => "10", "type" => "Sale", "code" => "Code 2", "amount" => 800
      }}
    ]

    changes = config_changes batch_size: 4

    docs.each { |d| changes.send(:process_row, d) }

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 1, sales.count
    assert_equal({ sale_id: "10", code: "Code 2", amount: 800, updated_at: nil }, sales.first)
    assert_sequence 124
  end

  def test_insert_sales_and_nested_entries
    doc = { "id" => 1, "seq" => 111, "doc" => {
      "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [
        { "price" => 500 }, { "price" => 100 }
      ]
    }}

    changes = config_changes batch_size: 5

    changes.send(:process_row, doc)

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 1, sales.count
    assert_equal({ sale_id: "50", code: "Code 1", amount: 600, updated_at: nil }, sales.first)

    entries = @database[:sale_entries].to_a
    assert_equal 2, entries.count
    assert_includes entries, sale_id: "50", price: 500
    assert_includes entries, sale_id: "50", price: 100

    assert_sequence 111
  end

  def test_insert_sales_and_single_nested_entry
    doc = { "id" => 1, "seq" => 111, "doc" => {
      "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => { "price" => 500 }
    }}

    changes = config_changes batch_size: 4

    changes.send(:process_row, doc)

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 1, sales.count
    assert_equal({ sale_id: "50", code: "Code 1", amount: 600, updated_at: nil }, sales.first)

    entries = @database[:sale_entries].to_a
    assert_equal 1, entries.count
    assert_includes entries, sale_id: "50", price: 500

    assert_sequence 111
  end

  def test_insert_and_update_sales_and_nested_entries
    docs = [
      { "id" => 1, "seq" => 111, "doc" => {
        "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [{ "price" => 500 }, { "price" => 100 }]
      }},
      { "id" => 1, "seq" => 112, "doc" => {
        "_id" => "50", "type" => "Sale", "code" => "Code 2", "amount" => 900, "entries" => [{ "price" => 300 }, { "price" => 600 }]
      }}
    ]

    changes = config_changes batch_size: 5

    docs.each { |d| changes.send(:process_row, d) }

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 1, sales.count
    assert_equal({ sale_id: "50", code: "Code 2", amount: 900, updated_at: nil }, sales.first)

    entries = @database[:sale_entries].to_a
    assert_equal 2, entries.count
    assert_includes entries, sale_id: "50", price: 300
    assert_includes entries, sale_id: "50", price: 600

    assert_sequence 112
  end

  def test_insert_and_update_sales_and_nested_entries_in_same_batch
    docs = [
      { "id" => 1, "seq" => 111, "doc" => {
        "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [{ "price" => 500 }, { "price" => 100 }]
      }},
      { "id" => 1, "seq" => 112, "doc" => {
        "_id" => "50", "type" => "Sale", "code" => "Code 2", "amount" => 900, "entries" => [{ "price" => 300 }, { "price" => 600 }]
      }}
    ]

    changes = config_changes batch_size: 10

    docs.each { |d| changes.send(:process_row, d) }

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 1, sales.count
    assert_equal({ sale_id: "50", code: "Code 2", amount: 900, updated_at: nil }, sales.first)

    entries = @database[:sale_entries].to_a
    assert_equal 2, entries.count
    assert_includes entries, sale_id: "50", price: 300
    assert_includes entries, sale_id: "50", price: 600

    assert_sequence 112
  end

  def test_insert_different_document_types
    docs = [
      { "id" => 1, "seq" => 111, "doc" => {
        "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [{ "price" => 500 }, { "price" => 100 }]
      }},
      { "id" => 2, "seq" => 112, "doc" => { "_id" => "3000", "type" => 'AnalyticEvent', "key" => "click", "value" => "yes" }},
      { "id" => 3, "seq" => 113, "doc" => {
        "_id" => "51", "type" => "Sale", "code" => "Code 2", "amount" => 900, "entries" => [{ "price" => 300 }, { "price" => 600 }]
      }},
      { "id" => 4, "seq" => 114,  "doc" => { "_id" => "3001", "type" => 'AnalyticEvent', "key" => "double-click", "value" => "too much" }}
    ]

    changes = config_changes batch_size: 7

    docs.each { |d| changes.send(:process_row, d) }

    changes.stop_consumer

    sales = @database[:sales].to_a
    assert_equal 2, sales.count
    assert_includes sales, sale_id: "50", code: "Code 1", amount: 600, updated_at: nil
    assert_includes sales, sale_id: "51", code: "Code 2", amount: 900, updated_at: nil

    entries = @database[:sale_entries].to_a
    assert_equal 4, entries.count
    assert_includes entries, sale_id: "50", price: 500
    assert_includes entries, sale_id: "50", price: 100
    assert_includes entries, sale_id: "51", price: 300
    assert_includes entries, sale_id: "51", price: 600

    events = @database[:analytic_events].to_a
    assert_equal 2, events.count
    assert_includes events, analytic_event_id: "3000", key: "click", value: "yes"
    assert_includes events, analytic_event_id: "3001", key: "double-click", value: "too much"

    assert_sequence 114
  end

  def test_delete_children
    changes = config_changes batch_size: 6

    changes.send(:process_row, { "id" => 1, "seq" => 111, "doc" => { "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [{ "price" => 500 }, { "price" => 100 }] }})
    changes.send(:process_row, { "id" => "50", "seq" => 112, "deleted" => true } )

    changes.stop_consumer

    assert_equal 0, @database[:sales].count
    assert_equal 0, @database[:sale_entries].count
    assert_sequence 112
  end

  protected

  def config_changes(opts)
    changes = CouchTap::Changes.new(couch_db: TEST_DB_ROOT, timeout: 60) do
      database db: 'sqlite:/', batch_size: opts.fetch(:batch_size)

      document type: 'Sale' do
        table :sales do
          column :audited_at, Time.now
          collection :entries do
            table :sale_entries, primary_key: false do
              column :audited_at, Time.now
            end
          end
        end
      end

      document type: 'AnalyticEvent' do
        table :analytic_events do
        end
      end
    end

    @database = changes.query_executor.database
    migrate_sample_database @database

    changes.send(:start_consumer)
    return changes
  end


  def migrate_sample_database(connection)
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

  def assert_sequence(expected)
    assert_equal expected, @database[:couch_sequence].where(name: "couch_tap").to_a.first[:seq]
  end
end
